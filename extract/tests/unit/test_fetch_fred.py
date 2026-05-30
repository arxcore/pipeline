import monitoring.exc_models as exc
import asyncio
import aiohttp
import pytest
from unittest.mock import AsyncMock, MagicMock
from pytest_mock import MockFixture, MockerFixture
from providers.fred import FREDProvider
from providers.fred.model import FREDRawResponse
from providers import BaseMetaModel

fake_respons = {"observations": [{"date": "2024-02-01", "value": "5.33"}]}
meta: BaseMetaModel = BaseMetaModel(
    id="FEDFUNDS",
    api="fred",
    calc="raw",
    start_year=2024,
    start_month=2,
    freq="monthly",
    unit="percent",
    description="fed rate",
)


@pytest.mark.asyncio
async def test_fetch_fred_succs(mocker: MockFixture):
    mock_respons = AsyncMock()
    mock_respons.status = 200
    mock_respons.raise_for_status = MagicMock()
    mock_respons.json = AsyncMock(return_value=fake_respons)
    mock = AsyncMock()
    mock.__aenter__ = AsyncMock(return_value=mock_respons)
    mock.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("aiohttp.ClientSession.get", return_value=mock)
    async with FREDProvider(api_key="123") as provider:
        data = await provider.fetch_data(meta)
        assert isinstance(data, FREDRawResponse)


@pytest.mark.asyncio
async def test_fetch_fred_error(mocker: MockerFixture):
    mock_respons = AsyncMock()
    mock_respons.raise_for_status = MagicMock(
        side_effect=aiohttp.ClientResponseError(
            status=400, request_info=MagicMock(), history=()
        )
    )
    mock = AsyncMock()
    mock.__aenter__ = AsyncMock(return_value=mock_respons)
    mock.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("aiohttp.ClientSession.get", return_value=mock)
    async with FREDProvider(api_key="213") as provider:
        with pytest.raises(exc.FREDRequestsError):
            await provider.fetch_data(meta)


@pytest.mark.asyncio
async def test_fetch_fred_retry(mocker: MockerFixture):
    """Retry status http 5xx Test"""
    mock_respons = AsyncMock()
    mock_respons.status = 200
    mock_respons.json = AsyncMock(return_value=fake_respons)
    mock_respons.raise_for_status = MagicMock(
        side_effect=[
            aiohttp.ClientResponseError(
                request_info=MagicMock(), status=500, history=()
            ),
            aiohttp.ClientResponseError(
                request_info=MagicMock(), status=500, history=()
            ),
            aiohttp.ClientResponseError(
                request_info=MagicMock(), status=500, history=()
            ),
            aiohttp.ClientResponseError(
                request_info=MagicMock(), status=500, history=()
            ),
            # succes respons
            None,
        ]
    )
    mock = AsyncMock()
    mock.__aenter__ = AsyncMock(return_value=mock_respons)
    mock.__aexit__ = AsyncMock(return_value=None)
    retry = mocker.patch("aiohttp.ClientSession.get", return_value=mock)

    async with FREDProvider(api_key="123") as provider:
        result = await provider.fetch_data(meta)
        assert isinstance(result, FREDRawResponse)
    assert retry.call_count == 5


async def test_semaphore_limit(mocker: MockerFixture):
    """Test limit concurent requests with semaphore"""

    current_con = 0
    max_con = 0

    async def tracked_operation_sem():
        nonlocal current_con, max_con
        current_con += 1
        max_con = max(max_con, current_con)
        await asyncio.sleep(0.1)
        current_con -= 1
        return fake_respons

    mock_respons = AsyncMock()
    mock_respons.status = 200
    mock_respons.raise_for_status = MagicMock()
    mock_respons.json = AsyncMock(side_effect=tracked_operation_sem)
    mock = AsyncMock()
    mock.__aenter__ = AsyncMock(return_value=mock_respons)
    mock.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("aiohttp.ClientSession.get", return_value=mock)
    async with FREDProvider(api_key="123") as provider:
        tasks = [provider.fetch_data(meta) for _ in range(100)]
        await asyncio.gather(*tasks)
        assert max_con == 5
