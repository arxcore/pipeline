from pathlib import PosixPath
from unittest.mock import AsyncMock
from pytest_mock import MockerFixture
from providers.ons.fetch import ONSProvider
from providers.ons.model import ONSConfigModel, OnsResult

DATA = OnsResult(
    path=PosixPath(
        "/home/arzswdy/sys/service/pipeline/downloads/ons/uk/price/D7OE_49b056b0_20260707_53697d7b.csv"
    ),
    ETag='"c189180ffb1b3cbd2d9b6bc14b26ad6cd4f241ea--gzip"',
)

meta = ONSConfigModel(
    code_name="D7OE",
    source="ons",
    calc="raw",
    url="https://www.ons.gov.uk/generator?format=csv&uri=/economy/inflationandpriceindices/timeseries/d7g7/mm23",
    description="CPI Annual Rate - All Items 2015=100",
    unit="%",
    freq="monthly",
)


async def test_fetch_ons(mocker: MockerFixture):
    mock = AsyncMock()
    mock.status = 200
    mock.raise_for_status = AsyncMock()
    mock_get = AsyncMock()
    mock_get.__aenter__ = AsyncMock(return_value=mock)
    mock_get.__aexit__ = AsyncMock(return_value=None)
    mock_get.headers = AsyncMock(return_value=mock)
    mocker.patch("aiohttp.ClientSession.get", return_value=mock_get)
    async with ONSProvider(fetch_db=AsyncMock(return_value=DATA)) as prov:
        # FIXME:
        # if "text/html" in r.headers.get("Content-Type", ""):
        # TypeError: Argument of type 'corontine' is not iterable
        result = await prov.fetch_data(
            meta=meta,
            category="price",
            country="uk",
            indicator_name="CPI_MoM",
        )
        assert isinstance(result, OnsResult)
