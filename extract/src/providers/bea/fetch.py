from datetime import datetime
import aiohttp
from src.providers.bea.model import BEAConfigModel
from providers import BaseMetaModel
import logging
from tenacity import (
    retry,
    stop_after_attempt,
    retry_if_exception,
    wait_exponential,
)
import monitoring.exc_models as exc
from typing import Any, Callable, cast
from providers.retry_http import Retryable
import asyncio
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class BEAProvider:
    """
    Example Debug Params:
    "method": "GetParameterValues",
    "datasetname": "ITA",
    "ParameterName": "AreaOrCountry",
    "Year": "ALL",
    "ResultFormat": "JSON",
    """

    def __init__(self, api_key: str | None = None, limit_requests: int = 5):
        self.api_key = api_key
        self.url = "https://apps.bea.gov/api/data"
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
    ) -> None:
        if self.session:
            await self.session.close()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=4, max=70),
        retry=retry_if_exception(cast(Callable[[BaseException], bool], Retryable)),
        reraise=True,
    )
    async def fetch_data(
        self,
        meta: BaseMetaModel,
        category: str | None = None,
        country: str | None = None,
    ) -> dict[str, Any]:
        """Fetch data from BEA API"""
        # validate BEAConfigModel
        if not isinstance(meta, BEAConfigModel):
            raise TypeError("BEAProvider expect BEAConfigModel got %s", type(meta))

        # Check api key if not exists
        if not self.api_key:
            raise exc.ResourceNotFound(f"{meta.source} apikey not found")

        if not meta.start_year:
            raise ValueError(
                "start year not found for %s, table %s", meta.dataset, meta.table
            )

        # build Start year and end year
        start_range = list(range(meta.start_year, datetime.now().year + 1))
        start_to_end = ",".join(str(y) for y in start_range)

        params: dict[str, str | None] = {
            "UserID": self.api_key,
            "method": "GetData",
            "Year": start_to_end,
            "Frequency": meta.freq,
            "ResultFormat": "JSON",
        }
        if meta.dataset == "ITA":
            params.update({"DataSetName": meta.dataset})
            params.update({"AreaOrCountry": "AllCountries"})
            params.update({"Indicator": meta.code_name})

        if meta.dataset == "NIPA":
            params.update({"DataSetName": meta.dataset})
            params.update({"TableName": meta.table})
            params.update({"LineNumber": meta.line_number})

        # skip None Paramas
        filter_params = {k: v for k, v in params.items() if v is not None}

        if not self.session:
            raise exc.BEARequestsError("connection HTTP BEA Session is Not Initialized")

        try:
            async with self.semaphore:
                # aiiohttp Context Manager
                async with self.session.get(
                    self.url,
                    params=filter_params,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as response:
                    # Retry if status code http >= 500
                    # 4xx, 5xx
                    response.raise_for_status()

                    # 1xx, 3xx, etc..
                    if response.status != 200:
                        raise exc.BEARequestsError(
                            "Unexpected Error Respons %s", response.status
                        )

                    logger.info("BEA HTTP status code %s", response.status)

                    try:
                        data = await response.json()

                        if data["BEAAPI"]["Results"].get("Error"):
                            raise exc.BEARequestsError(
                                data["BEAAPI"]["Results"]["Error"][
                                    "APIErrorDescription"
                                ]
                            )

                        logger.debug("respons json raw data BEA: %s", data)
                        logger.info(
                            "BEA raw data validation done.. %s data",
                            len(data["BEAAPI"]["Results"]["Data"]),
                        )

                        return data
                    except aiohttp.ContentTypeError as e:
                        # TODO:
                        # test if content type not json fromat
                        # wrap into providers handling
                        raise exc.BEARequestsError(f"Content Error {e}") from e
                    except ValidationError as e:
                        # wrap
                        raise exc.BEARequestsError(
                            f"Validation Response Error {e}"
                        ) from e
        except aiohttp.ClientResponseError as e:
            if e.status == 429:
                raise exc.RateLimit("Rate limit reached") from e
            elif e.status == 401:
                raise exc.AuthenticationError(
                    "Authentication error from requests"
                ) from e
            raise exc.BEARequestsError(f"HTTP Error {e.status}") from e
        except aiohttp.ClientError as e:
            raise exc.BEARequestsError(f"HTTP Client Error {e}") from e
