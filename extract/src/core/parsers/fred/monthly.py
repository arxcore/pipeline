from decimal import Decimal
from core.models.parsing_schemas import ParsedItems
from providers.fred import FREDRawResponse
from core.models import FinalresultParse, FinalresultFetcher
import logging
from core.parsers.registry import register, Providers, Frequency
import monitoring.exc_models as exc

logger = logging.getLogger(__name__)


@register(Providers.fred, Frequency.monthly)
def parse_monthly_fred(data: FinalresultFetcher) -> FinalresultParse:
    # Validation FinalresultFetcher
    RAW_DATA = FREDRawResponse.model_validate(data.fetch_result)

    logger.debug("FRED Parsing Accept %s Data", len(RAW_DATA.observations))
    result: list[ParsedItems] = []

    for entry in RAW_DATA.observations:
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

    logger.debug("Parsing monthly done with %s Data", len(result))
    return FinalresultParse(parse_result=result)
