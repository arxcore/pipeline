import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock
from pytest_mock import MockerFixture
import aiohttp
from providers.bls.fetch import BLSProvider
from providers.bls.model import BLSSeries
from providers.metamodel import BaseMetaModel
import monitoring.exc_models as exc


meta: BaseMetaModel = BaseMetaModel(
    id="CES0000000001",
    api="bls",
    start_month=2,
    start_year=2024,
    freq="monthly",
    calc="net",
    unit="USD",
    description="Net change NFP",
)
type = dict[
    str,
    str
    | int
    | list[str]
    | dict[str, list[dict[str, str | list[dict[str, str | list[str | None]]]]]],
]

fake_respons: type = {
    "status": "REQUESTS_FAILED",
    "responseTime": 1668,
    "message": ["Your Daily Query Limit has ben reached"],
    "Results": {
        "series": [
            {
                "seriesID": "CES0000000001",
                "data": [
                    {
                        "year": "2024",
                        "period": "M01",
                        "periodName": "January",
                        "value": "3.7",
                        "footnotes": [],
                    },
                    {
                        "year": "2024",
                        "period": "M02",
                        "periodName": "February",
                        "value": "3.9",
                        "footnotes": [],
                    },
                ],
            }
        ]
    },
}


@pytest.mark.asyncio
async def test_fetch_bls_succes(mocker: MockerFixture):
    """Test Fetch BLS Succes"""

    succs_respons: type = {
        "status": "REQUEST_SUCCEEDED",
        "responseTime": 1668,
        "message": ["Succs test respons"],
        "Results": {
            "series": [
                {
                    "seriesID": "CES0000000001",
                    "data": [
                        {
                            "year": "2024",
                            "period": "M01",
                            "periodName": "January",
                            "value": "3.7",
                            "footnotes": [],
                        },
                        {
                            "year": "2024",
                            "period": "M02",
                            "periodName": "February",
                            "value": "3.9",
                            "footnotes": [],
                        },
                    ],
                }
            ]
        },
    }
    mock_respons = AsyncMock()
    mock_respons.status = 200
    mock_respons.raise_for_status = MagicMock()
    mock_respons.json = AsyncMock(return_value=succs_respons)
    mock_get = AsyncMock()
    mock_get.__aenter__ = AsyncMock(return_value=mock_respons)
    mock_get.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("aiohttp.ClientSession.get", return_value=mock_get)
    async with BLSProvider(api_key="test") as provider:
        result = await provider.fetch_data(meta)
        assert isinstance(
            result,
            BLSSeries,
        )


@pytest.mark.asyncio
async def test_fetch_bls_error(mocker: MockerFixture):
    """Error Test with ClientResponseError"""
    mock_respons = AsyncMock()
    mock_respons.raise_for_status = MagicMock(
        side_effect=aiohttp.ClientResponseError(
            status=429, request_info=MagicMock(), history=()
        )
    )
    mock = AsyncMock()
    mock.__aenter__ = AsyncMock(return_value=mock_respons)
    mock.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("aiohttp.ClientSession.post", return_value=mock)

    async with BLSProvider(api_key="123") as provider:
        with pytest.raises(exc.RateLimit):
            await provider.fetch_data(meta)


@pytest.mark.asyncio
async def test_bls_retry_http_5xx(mocker: MockerFixture):
    """ "Retry http status code >= 500 Test"""
    retry_respons: type = {
        "status": "REQUEST_SUCCEEDED",
        "responseTime": 1668,
        "message": ["success test respons"],
        "Results": {
            "series": [
                {
                    "seriesID": "CES0000000001",
                    "data": [
                        {
                            "year": "2024",
                            "period": "M01",
                            "periodName": "January",
                            "value": "3.7",
                            "footnotes": [],
                        },
                        {
                            "year": "2024",
                            "period": "M02",
                            "periodName": "February",
                            "value": "3.9",
                            "footnotes": [],
                        },
                    ],
                }
            ]
        },
    }
    mock_respons = AsyncMock()
    mock_respons.status = 200
    mock_respons.json = AsyncMock(return_value=retry_respons)
    mock_respons.raise_for_status = MagicMock(
        side_effect=[
            aiohttp.ClientResponseError(
                status=500, request_info=MagicMock(), history=()
            ),
            aiohttp.ClientResponseError(
                status=500, request_info=MagicMock(), history=()
            ),
            aiohttp.ClientResponseError(
                status=500, request_info=MagicMock(), history=()
            ),
            aiohttp.ClientResponseError(
                status=500, request_info=MagicMock(), history=()
            ),
            # Success Response
            None,
        ]
    )
    mock = AsyncMock()
    mock.__aenter__ = AsyncMock(return_value=mock_respons)
    mock.__aexit__ = AsyncMock(return_value=None)
    retry = mocker.patch("aiohttp.ClientSession.post", return_value=mock)
    async with BLSProvider(api_key="123") as provider:
        result = await provider.fetch_data(meta)
        assert isinstance(result, BLSSeries)
    assert retry.call_count == 5


@pytest.mark.asyncio
async def test_daily_limit_query(mocker: MockerFixture):
    """Handling request  daily limit reched"""
    query_limit_respons: type = {
        "status": "REQUESTS_FAILED",
        "responseTime": 1668,
        "message": ["Your Daily Query Limit has ben reached"],
        "Results": {
            "series": [
                {
                    "seriesID": "CES0000000001",
                    "data": [
                        {
                            "year": "2024",
                            "period": "M01",
                            "periodName": "January",
                            "value": "3.7",
                            "footnotes": [],
                        },
                        {
                            "year": "2024",
                            "period": "M02",
                            "periodName": "February",
                            "value": "3.9",
                            "footnotes": [],
                        },
                    ],
                }
            ]
        },
    }

    mock_respons = AsyncMock()
    mock_respons.status = 200
    mock_respons.raise_for_status = MagicMock()
    mock_respons.json = AsyncMock(return_value=query_limit_respons)
    mock = AsyncMock()
    mock.__aenter__ = AsyncMock(return_value=mock_respons)
    mock.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("aiohttp.ClientSession.post", return_value=mock)
    async with BLSProvider(api_key="test") as provider:
        with pytest.raises(exc.RateLimit):
            await provider.fetch_data(meta)


@pytest.mark.asyncio
async def test_semaphore_limit_concurently(mocker: MockerFixture):
    """Test Semaphore Limit Concurently"""
    succs_respons: type = {
        "status": "REQUEST_SUCCEEDED",
        "responseTime": 1668,
        "message": ["Succs test respons"],
        "Results": {
            "series": [
                {
                    "seriesID": "CES0000000001",
                    "data": [
                        {
                            "year": "2024",
                            "period": "M01",
                            "periodName": "January",
                            "value": "3.7",
                            "footnotes": [],
                        },
                        {
                            "year": "2024",
                            "period": "M02",
                            "periodName": "February",
                            "value": "3.9",
                            "footnotes": [],
                        },
                    ],
                }
            ]
        },
    }
    current_count = 0
    max_concurrent = 0

    async def tracked_operation():
        nonlocal current_count, max_concurrent
        current_count += 1
        max_concurrent = max(max_concurrent, current_count)
        await asyncio.sleep(0.1)  # Simulate some work
        current_count -= 1
        return succs_respons

    mock_respons = AsyncMock()
    mock_respons.status = 200
    mock_respons.raise_for_status = MagicMock()
    mock_respons.json = AsyncMock(side_effect=tracked_operation)
    mock = AsyncMock()
    mock.__aenter__ = AsyncMock(return_value=mock_respons)
    mock.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("aiohttp.ClientSession.post", return_value=mock)
    async with BLSProvider(api_key="123") as provider:
        # check semaphore limit is expected to be 5
        tasks = [provider.fetch_data(meta) for _ in range(300)]
        await asyncio.gather(*tasks)
        assert max_concurrent == 5
