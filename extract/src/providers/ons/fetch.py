import asyncio
from typing import cast, Callable
import random
import aiohttp
import logging
from providers.metamodel import BaseMetaModel
from pathlib import Path
from datetime import datetime
import hashlib
from src.providers.ons.model import ONSConfigModel
import monitoring.exc_models as exc
import uuid
import aiofiles
from tenacity import (
    retry,
    stop_after_attempt,
    retry_if_exception,
    wait_exponential,
)
from providers.retry_http import Retryable

logger = logging.getLogger(__name__)


class ONSProvider:
    def __init__(self, limit_requests: int = 5):
        self.semaphore = asyncio.Semaphore(limit_requests)
        self.session: aiohttp.ClientSession | None = None

        pass

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        # await self.csv.__aenter__()
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
        retry=retry_if_exception(cast(Callable[[BaseException], bool], Retryable)),
        reraise=True,
    )
    async def fetch_data(
        self, meta: BaseMetaModel, category: str, country: str
    ) -> Path | None:
        """fetch data ONSProvider"""
        # validate ONSConfigModel
        if not isinstance(meta, ONSConfigModel):
            raise TypeError("ONSProvider expect ONSConfigModel got %s", type(meta))

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

        base_path = (
            Path(__file__).resolve().parents[4]
            / "downloads"
            / meta.source
            / country
            / category
        )
        base_path.mkdir(parents=True, exist_ok=True)
        final_path = base_path / name
        tmp_path = final_path.with_suffix(".tmp")

        # exit if file already exists
        if final_path.exists():
            logger.info("File already exists %s, skiping downloads -_", final_path.name)
            return final_path

        if not self.session:
            raise aiohttp.client.ClientError("connection http session not initialized")

        logger.info(
            "Waiting for semaphore to Downloads file %s: %s", meta.code_name, ext
        )
        # limit concurency downloads files
        async with self.semaphore:
            logger.info(
                "Acquired for semaphore - downloading %s (active slot: %d)",
                meta.code_name,
                self.semaphore._value + 1,
            )
            # delay between requests
            await asyncio.sleep(random.uniform(3, 8))
            try:
                async with self.session.get(
                    meta.url, timeout=aiohttp.ClientTimeout(total=30)
                ) as r:
                    r.raise_for_status()
                    if "text/html" in r.headers.get("Content-Type", ""):
                        raise exc.FetchDataError(
                            "Expected file, got HTML from ONS for %s ", meta.code_name
                        )

                    async with aiofiles.open(tmp_path, "wb") as f:
                        async for chunk in r.content.iter_chunked(8192 * 10):
                            await f.write(chunk)
                # rename file
                tmp_path.rename(final_path)
                logger.info("Succesfully downloads file %s", final_path.name)

                return final_path

            except aiohttp.ClientResponseError as e:
                # error http 4xx, 5xx
                logger.error(
                    "HTTP Failed downloads file %s: %s, %s",
                    meta.code_name,
                    e.status,
                    e.message,
                )
                if e.status == 429:
                    logger.warning("Rate limit reached will retry.. %s", meta.code_name)
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
