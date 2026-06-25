from decimal import Decimal
import logging
from core.models.parsing_schemas import ParsedItems
from core.parsers.registry import register, Providers, Frequency
from core.models import ParseResult, ApiResult
from providers.fred.model import FREDRawResponse
import monitoring.exc_models as exc

logger = logging.getLogger(__name__)


@register(Providers.fred, Frequency.weekly)
def parse_weekly_fred(data: ApiResult) -> ParseResult:
    RAW = FREDRawResponse.model_validate(data.source_data)

    logger.debug("FRED Parsing Accept %s Data", len(RAW.observations))
    result: list[ParsedItems] = []

    for entry in RAW.observations:
        if entry.value in ["-", " ", ".", "NA", "N/A"]:
            logger.warning("invalid format value on parsing for date %s", entry.date)
            continue
        try:
            result.append(ParsedItems(date_key=entry.date, value=Decimal(entry.value)))
        except ValueError as e:
            logger.error(f"canot convert value for date: {entry.date} {e}")
            continue
        except Exception as e:
            raise exc.FREDParserError(f"Parsing FRED Unknown ERROR {e}") from e

    logger.debug("Parsing weekly done with %s Data", len(result))
    return ParseResult(parse_result=result)
