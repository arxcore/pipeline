import asyncio
from datetime import datetime
import hashlib
import logging
from pathlib import Path
import random
from typing import Callable, cast
import uuid

import aiofiles
import aiohttp

from core.models.pipeline_schemas import FilePathResult
import monitoring.exc_models as exc
from providers.metamodel import BaseMetaModel
from providers.ons.model import ONSConfigModel
from providers.retry_http import Retryable
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential
from upload.postgres.fetch_db import FetchDB

logger = logging.getLogger(__name__)


class ONSProvider:
    def __init__(
        self,
        fetch_db: FetchDB,
        limit_requests: int = 1,
    ):
        self.semaphore = asyncio.Semaphore(limit_requests)
        self.session: aiohttp.ClientSession | None = None
        self.fetch_db = fetch_db

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ):
        if self.session:
            await self.session.close()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=4, min=8, max=70),
        retry=retry_if_exception(cast(Callable[[BaseException], bool], Retryable())),
        reraise=True,
    )
    async def fetch_data(
        self, meta: BaseMetaModel, category: str, country: str, indicator_name: str
    ) -> FilePathResult | None:
        """fetch data ONSProvider"""
        # validate ONSConfigModel
        if not isinstance(meta, ONSConfigModel):
            raise TypeError(f"ONSProvider expect ONSConfigModel got {type(meta)} ")
        if not meta.code_name:
            raise ValueError(f"code name not found for {meta.code_name}")

        # build naming file
        ext = None
        if "format=csv" in meta.url.lower():
            ext = ".csv"
        elif ".xlsx" in meta.url.lower():
            ext = ".xlsx"
        elif ".xls" in meta.url.lower():
            ext = ".xls"
        else:
            logger.warning("Unknwon file format url %s", meta.url)
            return None

        # build uniq file name
        url_hash = hashlib.md5(meta.url.encode("utf-8")).hexdigest()[:8]
        uniq = uuid.uuid4().hex[:8]
        timestamp = datetime.now().strftime("%Y%m%d")
        name = f"{meta.code_name}_{url_hash}_{timestamp}_{uniq}{ext}"

        # create dir
        base_path = (
            Path(__file__).resolve().parents[4]
            / "downloads"
            / meta.source
            / country
            / category
        )
        base_path.mkdir(parents=True, exist_ok=True)
        final_path = base_path / name

        # rename to tmp before finish downloads
        tmp_path = final_path.with_suffix(".tmp")

        logger.info(
            "code_name %s, indicator %s, source %s",
            meta.code_name,
            indicator_name,
            meta.source,
        )

        # check db if etag exists
        etag_load = await self.fetch_db.load_etag(meta, indicator_name)

        # check if file still fresh
        header: dict[str, str | None] = {}
        if etag_load:
            # check etag if exists
            if etag_load.etag:
                header["If-None-Match"] = etag_load.etag
            else:
                logger.warning(
                    "No etag found on database etag: %s, %s",
                    etag_load.etag,
                    indicator_name,
                )

        if not self.session:
            raise aiohttp.client.ClientError("connection http session not initialized")

        logger.info(
            "Waiting for semaphore to Downloads file %s: %s", meta.code_name, ext
        )
        # limit concurrency downloads files
        async with self.semaphore:
            logger.info(
                "Acquired for semaphore - downloading %s (active slot: %d)",
                meta.code_name,
                self.semaphore._value + 1,
            )
            # delay between requests - Fix 2: increased from 3-8 to 15-30
            await asyncio.sleep(random.uniform(15, 30))

            filters = {k: v for k, v in header.items() if v is not None}

            # check if filter is not none before use headers
            headers = filters if filters else None

            # Flag to track if we need to re-download without etag
            need_redownload = False
            saved_etag = None

            # Try download with etag header first
            try:
                async with self.session.get(
                    meta.url, timeout=aiohttp.ClientTimeout(total=60), headers=headers
                ) as r:
                    # Handle 304 Not Modified
                    if r.status == 304:
                        logger.info(
                            "File already fresh not fucking change for %s: %s, status: %s",
                            indicator_name,
                            meta.code_name,
                            r.status,
                        )
                        # check if etag is not None
                        if etag_load:
                            # check if file still exists locally
                            if etag_load.file_path.exists():
                                return FilePathResult(
                                    path=etag_load.file_path, ETag=etag_load.etag
                                )
                            # File missing from disk, need to re-download without etag header
                            logger.warning(
                                "no file path found in local disk %s: %s",
                                indicator_name,
                                meta.code_name,
                            )
                            need_redownload = True
                            saved_etag = r.headers.get("ETag")
                    else:
                        r.raise_for_status()

                    if "text/html" in r.headers.get("Content-Type", ""):
                        raise exc.FetchDataError(
                            "Expected file, got HTML from ONS for %s ", meta.code_name
                        )

                    async with aiofiles.open(tmp_path, "wb") as f:
                        async for chunk in r.content.iter_chunked(8192 * 10):
                            await f.write(chunk)

                    saved_etag = r.headers.get("ETag")

            except aiohttp.ClientResponseError as e:
                if need_redownload:
                    # Re-download without etag header (fix for nested context manager issue)
                    logger.info("RE-Downloading %s without etag", indicator_name)
                    async with self.session.get(
                        meta.url, timeout=aiohttp.ClientTimeout(total=60)
                    ) as r:
                        r.raise_for_status()
                        if "text/html" in r.headers.get("Content-Type", ""):
                            raise exc.FetchDataError(
                                "Expected file, got HTML from ONS for %s ",
                                meta.code_name,
                            )
                        async with aiofiles.open(tmp_path, "wb") as f:
                            async for chunk in r.content.iter_chunked(8192 * 10):
                                await f.write(chunk)
                        saved_etag = r.headers.get("ETag")
                else:
                    # error http 4xx, 5xx
                    logger.error(
                        "HTTP Failed downloads file %s: %s, %s",
                        meta.code_name,
                        e.status,
                        e.message,
                    )
                    if e.status == 429:
                        logger.warning(
                            "Rate limit reached will retry.. %s", meta.code_name
                        )
                        raise e
                    elif e.status == 401:
                        raise exc.AuthenticationError(
                            "Authentication error from requests"
                        ) from e

                    if tmp_path.exists():
                        tmp_path.unlink()
                    raise
            except aiohttp.ClientError as e:
                # connection error, refused, timeout
                logger.error("Failied downloads file %s: %s", meta.code_name, str(e))
                if tmp_path.exists():
                    tmp_path.unlink()

                raise
            except exc.FetchDataError as e:
                logger.error("Failied downloads file %s: %s", meta.code_name, str(e))
                if tmp_path.exists():
                    tmp_path.unlink()
                raise
            except asyncio.CancelledError as e:
                logger.error(
                    "downloads canceled for %s: %s - cleaning-up..",
                    meta.code_name,
                    str(e),
                )
                if tmp_path.exists():
                    tmp_path.unlink()
                raise
            except Exception as e:
                logger.exception(
                    "Unhandel exception while downloading %s: %s",
                    meta.code_name,
                    str(e),
                )
                if tmp_path.exists():
                    tmp_path.unlink()
                raise
            finally:
                logger.info("Released semaphore for %s", meta.code_name)

            # rename file
            tmp_path.rename(final_path)

            # Fix 3: verify file integrity
            if not final_path.exists() or final_path.stat().st_size == 0:
                if final_path.exists():
                    final_path.unlink()
                raise exc.FetchDataError(
                    f"Downloaded file for {meta.code_name} is empty or missing."
                )

            # remove unfresh link registry db and local file
            if etag_load:
                path = etag_load.file_path
                if path.exists():
                    logger.info(
                        "File Path %s found, for %s: %s, deleting...",
                        path,
                        meta.code_name,
                        indicator_name,
                    )
                    path.unlink()

                    # remove file registry duplicate handling before fresh downloads
                    await self.fetch_db.delete_path_file_registry(meta, indicator_name)

            logger.info("Succesfully downloads file %s", final_path.name)

            # return fresh path registry and fresh local file disk
            return FilePathResult(path=final_path, ETag=saved_etag)
