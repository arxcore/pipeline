from core.models import ApiResult, ParseResult
import logging
from core.parsers.registry import PARSE_REGISTER
import monitoring.exc_models as exc

logger = logging.getLogger(__name__)


class ParseProcessors:
    """Handling Parse Data with Diverent Frequency"""

    def parse_data(
        self, raw_data: ApiResult, api: str, freq: str | None = None
    ) -> ParseResult:
        """Process Parse Data by api Type"""

        logger.info("Parsing data for %s with frequency %s", api, freq)
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
