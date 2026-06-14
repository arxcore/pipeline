from decimal import Decimal
from core.models import FinalresultParse, FinalresultFetcher
from providers.bea.model import BEARawRespons
import logging
from core.parsers.registry import Frequency, Providers, register
import monitoring.exc_models as exc
from core.models.parsing_schemas import ParsedItems

logger = logging.getLogger(__name__)


@register(Providers.bea, Frequency.qsa)
def parse_qsa_bea(data: FinalresultFetcher) -> FinalresultParse:
    """
    Parse data QuarterlySeasonallyAdjusted
    Return dict[str, float]
    """
    RAW_DATA = BEARawRespons.model_validate(data.fetch_result)

    logger.debug(
        "Parse data BEA QSA, Accept %s data, Sample: %s",
        len(RAW_DATA.BEAAPI.Results.Data),
        RAW_DATA.BEAAPI.Results.Data,
    )
    # parse_data: dict[str, float] = {}
    parse_data: list[ParsedItems] = []
    missing_value: list[str] = []
    for item in RAW_DATA.BEAAPI.Results.Data:
        date = item.TimePeriod
        year, quarter = date.split("Q")
        month = int(quarter) * 3
        date_key = f"{year}-{month:02d}-01"
        str_value = item.DataValue
        if str_value in ["-", "N/A", "NA", "", " "]:
            logger.warning("Skipping Parsing data: %s", date)
            missing_value.append(date)
            continue

        try:
            values = Decimal(str_value)
            parse_data.append(ParsedItems(date_key=date_key, value=values))
        except ValueError as e:
            logger.error(f"Parse Error for data: {date} with value: {str_value}-{e}")
            continue

        except Exception as e:
            raise exc.BEAParserError(f"Parsing BEA QSA Unknown ERROR {e}") from e

    logger.debug(
        "Parse QSA data Done Sampl data  %s",
        (parse_data, len(parse_data)),
    )

    if missing_value:
        logger.warning("Total Missing value for data %s", len(missing_value))

    return FinalresultParse(parse_result=parse_data)
