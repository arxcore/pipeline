from datetime import datetime
from pydantic import ValidationError
from tenacity import (
    retry,
    wait_exponential,
    retry_if_exception,
    stop_after_attempt,
)
from providers.metamodel import BaseMetaModel
from providers.bls.model import BLSRawResponsedata, BLSSeries
import logging
import aiohttp
import monitoring.exc_models as exc
from providers.retry_http import Retryable
from typing import Callable, cast
import asyncio
from providers.share_state import ExternalLimit

logger = logging.getLogger(__name__)


class BLSProvider:
    def __init__(self, api_key: str | None = None, limit_requests: int = 5):
        self.api_key = api_key
        self.url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
        self.session: aiohttp.ClientSession | None = None
        self.semaphore = asyncio.Semaphore(limit_requests)  # Limit concurrent requests

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
        wait=wait_exponential(multiplier=2, min=2, max=60),
        retry=retry_if_exception(cast(Callable[[BaseException], bool], Retryable)),
        reraise=True,
    )
    async def fetch_data(
        self,
        meta: BaseMetaModel,
    ) -> BLSSeries | None:
        limit_event = ExternalLimit.get(meta.source)
        # check the new assigment has arrived to the event loop and if the limit is reached before acquiring the semaphore
        if limit_event.is_set():
            logger.warning(
                "%s daily limit already reached, skipping before entry semaphore queue.. skipping",
                meta.source,
            )
            return None

        try:
            async with self.semaphore:
                # check if the limit is reached after acquiring the semaphore to avoid making unnecessary requests to the server
                if limit_event.is_set():
                    logger.warning(
                        "%s limit Trigered while waiting in semaphore... skipping",
                        meta.source,
                    )
                    return None
                # chekc api key
                if not self.api_key:
                    raise exc.ResourceNotFound(f"{meta.source} apikey not found")
                # end year
                end_year = datetime.now().year

                # build payload
                payload: dict[str, list[str] | str | int] = {
                    "seriesid": [meta.code_name],
                    "apikey": self.api_key,
                    "startyear": meta.start_year,
                    "endyear": end_year,
                }
                # check session if not exists
                if not self.session:
                    raise exc.BLSRequestsError(
                        "connection HTTP BLS Session not initialized"
                    )

                async with self.session.post(
                    self.url, json=payload, timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    # 4xx, 5xx
                    response.raise_for_status()

                    # handling respons 1xx, 3xx
                    if response.status != 200:
                        raise exc.BLSRequestsError(
                            "Unexpected Error Respons %s", response.status
                        )

                    logger.info("BLS HTTP status code %s", response.status)
                    try:
                        data = await response.json()

                        if data.get("status") != "REQUEST_SUCCEEDED":
                            msg = data.get("message", ["Unknown api error"])
                            if "daily threshold" in msg[0]:
                                logger.warning(
                                    "JSON respons msg, Daily limit reached for %s, skipping requests server until next day..",
                                    meta.source,
                                )
                                limit_event.set()
                                return None
                            raise exc.BLSRequestsError(
                                msg[0] if msg else "Unknown api error"
                            )

                        logger.debug("json respons raw data BLS: %s", data)
                        result = BLSRawResponsedata.model_validate(data).Results
                        logger.info(
                            "BLS raw data validation done..  %s data",
                            len(result.series[0].data),
                        )
                        return result

                    except ValidationError as e:
                        raise exc.BLSRequestsError(
                            f"Validation Response Error {e}"
                        ) from e
                    except aiohttp.ContentTypeError as e:
                        raise exc.BLSRequestsError(f"Content Error {e}") from e
        except aiohttp.ClientResponseError as e:
            if e.status == 429:
                raise exc.RateLimit("Too Many Requests") from e
            elif e.status == 401:
                raise exc.AuthenticationError("Unauthorized request respons") from e

            raise exc.BLSRequestsError(f"HTTP Error: {e.status}") from e
        except aiohttp.ClientError as e:
            raise exc.BLSRequestsError(f"HTTP Client Error: {e}") from e
