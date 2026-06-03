from config.settings import Resources
from providers.bls import BLSProvider
from providers.fred import FREDProvider
from providers.bea import BEAProvider
from providers import BaseMetaModel
from core.models import FinalresultFetcher
import logging
import monitoring.exc_models as exc

logger = logging.getLogger(__name__)


class RawProcessors:
    """Raw Processors Fetch ALL Prioviders Data"""

    def __init__(self, resource: Resources | None = None):
        self.resource = resource or Resources()
        self.providerd = {
            "bls": BLSProvider(api_key=self.resource.bls_api_key),
            "bea": BEAProvider(api_key=self.resource.bea_api_key),
            "fred": FREDProvider(api_key=self.resource.fred_api_key),
        }

    async def __aenter__(self):
        # TODO:
        # Implement a better way to handle multiple provider session opening and closing
        # Currently, if one provider fails to open, it will close all previously opened sessions. This can be improved by implementing a more robust error handling mechanism.
        # sequentinel session oppening--bootleneck, refactor and move to parallel session opening with asyncio.gather and handle exceptions accordingly
        open_session: list[str] = []
        for p in self.providerd:
            try:
                await self.providerd[p].__aenter__()
                open_session.append(p)
            except Exception:
                for o in reversed(open_session):
                    await self.providerd[o].__aexit__(None, None, None)
                logger.exception("Error Opening Provider %s", p)
                raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> None:
        for p in self.providerd:
            try:
                await self.providerd[p].__aexit__(exc_type, exc_val, exc_tb)
            except Exception:
                logger.exception("Error Closing Provider %s", p)
                continue

    async def process_raw_data(self, meta: BaseMetaModel) -> FinalresultFetcher | None:
        """Fetch Raw Data from ALL Prioviders"""
        logger.info("-" * 50)
        logger.info("Fetch data from %s, code %s", meta.source.upper(), meta.code_name)
        logger.info(
            "start year %s, start month %s, frequency %s",
            meta.start_year,
            meta.start_month,
            meta.freq,
        )
        if meta.source not in self.providerd:
            raise KeyError(f"Source {meta.source} not Found")
        try:
            providers_cls = self.providerd[meta.source]
            raw_data = await providers_cls.fetch_data(meta)
            if raw_data is None:
                logger.warning(
                    "No data fetched for Source %s, Code %s",
                    meta.source,
                    meta.code_name,
                )
                return None

            return FinalresultFetcher(source=meta.source, fetch_result=raw_data)

        except exc.FetchDataError:
            logger.exception("Error Fetch Data from Source %s", meta.source)
            raise
