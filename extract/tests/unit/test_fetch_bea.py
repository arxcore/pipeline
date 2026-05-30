import pytest
from pytest_mock import MockFixture
from unittest.mock import AsyncMock, MagicMock
from providers.bea.fetch import BEAProvider
from providers.metamodel import BaseMetaModel
import aiohttp
from providers.bea.model import BEARawRespons
import monitoring.exc_models as exc
import asyncio

meta: BaseMetaModel = BaseMetaModel(
    id="BalCurrAcct",
    api="bea",
    calc="raw",
    start_year=2020,
    start_month=1,
    freq="QSA",
    unit="billion",
    description="description",
)


@pytest.mark.asyncio
async def test_fetch_bea_succs(mocker: MockFixture):
    mocker_respons = AsyncMock()
    mocker_respons.status = 200
    mocker_respons.raise_for_status = MagicMock()
    mocker_respons.json = AsyncMock(
        return_value={
            "BEAAPI": {
                "Results": {"Data": [{"TimePeriod": "2020Q1", "DataValue": "234"}]}
            }
        }
    )
    mocker_get = AsyncMock()
    mocker_get.__aenter__ = AsyncMock(return_value=mocker_respons)
    mocker_get.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("aiohttp.ClientSession.get", return_value=mocker_get)

    async with BEAProvider(api_key="123") as provider:
        result = await provider.fetch_data(meta)
        assert isinstance(result, BEARawRespons)


@pytest.mark.asyncio
async def test_fetch_bea_error(mocker: MockFixture):
    mock_respons = AsyncMock()
    mock_respons.raise_for_status = MagicMock(
        side_effect=aiohttp.ClientResponseError(
            status=400, request_info=MagicMock(), history=()
        ),
    )
    mock = AsyncMock()
    mock.__aenter__ = AsyncMock(return_value=mock_respons)
    mock.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("aiohttp.ClientSession.get", return_value=mock)

    async with BEAProvider(api_key="123") as provider:
        with pytest.raises(exc.BEARequestsError):
            await provider.fetch_data(meta)


@pytest.mark.asyncio
async def test_fetch_bea_retry_5xx(mocker: MockFixture):
    mock_respons = AsyncMock()
    mock_respons.status = 200
    mock_respons.json = AsyncMock(
        return_value={
            "BEAAPI": {
                "Results": {"Data": [{"TimePeriod": "2020Q1", "DataValue": "023"}]}
            }
        }
    )
    mock_respons.raise_for_status = MagicMock(
        side_effect=[
            aiohttp.ClientResponseError(
                request_info=MagicMock(), history=(), status=500
            ),
            aiohttp.ClientResponseError(
                request_info=MagicMock(), history=(), status=500
            ),
            aiohttp.ClientResponseError(
                request_info=MagicMock(), history=(), status=500
            ),
            aiohttp.ClientResponseError(
                request_info=MagicMock(), history=(), status=500
            ),
            None,
        ]
    )
    mock = AsyncMock()
    mock.__aenter__ = AsyncMock(return_value=mock_respons)
    mock.__aexit__ = AsyncMock(return_value=None)
    retry_session = mocker.patch("aiohttp.ClientSession.get", return_value=mock)

    async with BEAProvider(api_key="123") as provider:
        result = await provider.fetch_data(meta)
        assert isinstance(result, BEARawRespons)
    assert retry_session.call_count == 5


@pytest.mark.asyncio
async def tests_semaphore_limit(mocker: MockFixture):
    """Test that the semaphore limits concurrent requests"""
    return_value = {
        "BEAAPI": {"Results": {"Data": [{"TimePeriod": "2020Q1", "DataValue": "023"}]}}
    }
    current_calls = 0
    max_concurent_calls = 0

    async def tesing_concurent_calls():
        nonlocal current_calls, max_concurent_calls
        current_calls += 1
        max_concurent_calls = max(max_concurent_calls, current_calls)
        await asyncio.sleep(0.1)  # Simulate network delay
        current_calls -= 1
        return return_value

    mock_respons = AsyncMock()
    mock_respons.status = 200
    mock_respons.raise_for_status = MagicMock()
    mock_respons.json = AsyncMock(side_effect=tesing_concurent_calls)
    mock = AsyncMock()
    mock.__aenter__ = AsyncMock(return_value=mock_respons)
    mock.__aexit__ = AsyncMock(return_value=None)
    mocker.patch("aiohttp.ClientSession.get", return_value=mock)

    async with BEAProvider(api_key="123") as provider:
        tasks = [provider.fetch_data(meta) for _ in range(100)]
        await asyncio.gather(*tasks)
        assert max_concurent_calls == 5
