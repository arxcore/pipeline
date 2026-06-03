from core.models import FinalresultFetcher, FinalresultParse
import logging
from core.parsers.registry import PARSE_REGISTER
import monitoring.exc_models as exc

logger = logging.getLogger(__name__)


class ParseProcessors:
    """Handling Parse Data with Diverent Frequency"""

    def __call__(
        self, raw_data: FinalresultFetcher, api: str, freq: str | None = None
    ) -> FinalresultParse:
        """Process Parse Data by api Type"""
        logger.info("-" * 50)

        try:
            if api not in PARSE_REGISTER:
                raise exc.RoutingError(f"{api} not found in register parse")

            elif freq not in PARSE_REGISTER[api]:
                raise exc.RoutingError(
                    f"frequency {freq} not found in register parse for {api}"
                )
            parsed = PARSE_REGISTER[api][freq]
            return parsed(raw_data)
        except exc.RoutingError:
            logger.exception("Routing failed for api %s freq %s", api, freq)
            raise
