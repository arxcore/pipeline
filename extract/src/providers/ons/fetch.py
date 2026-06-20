import asyncio
import aiohttp
import logging
from providers.metamodel import BaseMetaModel
from pathlib import Path
from datetime import datetime
import hashlib
from src.providers.ons.model import ONSConfigModel

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

        PATH = Path(__file__).resolve().parents[4]
        DOWNLOADS_PATH = PATH / "downloads" / meta.source / country / category
        DOWNLOADS_PATH.mkdir(parents=True, exist_ok=True)

        url_byte = meta.url.encode("utf-8")
        shord_id = hashlib.md5(url_byte).hexdigest()[:8]
        timestamp = datetime.now().strftime("%Y%m%d")
        name = f"{meta.code_name}_{shord_id}_{timestamp}{ext}"
        file = DOWNLOADS_PATH / name

        if file.exists():
            logger.info("File already exists %s, skiping downloads", file.name)
            return file

        if not self.session:
            raise aiohttp.client.ClientError("connection http session not initialized")

        try:
            async with self.session.get(
                meta.url, timeout=aiohttp.ClientTimeout(total=30)
            ) as r:
                r.raise_for_status()
                with open(file, "wb") as f:
                    async for chunk in r.content.iter_chunked(8192 * 10):
                        f.write(chunk)
            logger.info("Succesfully downloads file %s", file.name)

            return file

        except aiohttp.ClientResponseError as e:
            # error http 4xx, 5xx
            logger.error(
                "HTTP Failed downloads file %s: %s, %s",
                meta.code_name,
                e.status,
                e.message,
            )
            if file.exists():
                file.unlink()
            raise
        except aiohttp.ClientError as e:
            # connection error, refused, timeout
            logger.error("Failied downloads file %s: %s", meta.code_name, str(e))
            if file.exists():
                file.unlink()

            raise
