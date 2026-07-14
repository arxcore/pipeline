from unittest.mock import AsyncMock, MagicMock
from core.flows._fetch import orchest_all_fetch
from tests.respon.run_all_tuple import TUPLE_DATA_PIPELINE_RESULT
import logging

logger = logging.getLogger(__name__)

data = TUPLE_DATA_PIPELINE_RESULT


async def test_fetch_orchest_all_fetch():
    mock = AsyncMock()
    mock.run_all = AsyncMock(return_value=data)
    mock_get = AsyncMock()
    mock_get.__aenter__ = AsyncMock(return_value=mock)
    mock_get.__aexit__ = AsyncMock(return_value=None)

    r = await orchest_all_fetch(
        manager=mock,
        source=[""],
        persist_raw=True,
        country=MagicMock(),
        indicator=MagicMock(),
    )
    assert isinstance(r, AsyncMock)
