from decimal import Decimal
import logging
from core.models import ApiResult
from core.models.parsing_schemas import ParsedItems
from core.models.pipeline_schemas import ParseResult
from core.parsers.registry import Frequency, Providers, register
from providers.bea.model import BEARawRespons
import monitoring.exc_models as exc

logger = logging.getLogger(__name__)


@register(Providers.bea, Frequency.m)
def parse_monthly_bea(data: ApiResult) -> ParseResult:
    """Parse data monthly BEA return ParseResult"""
    config = ApiResult.model_validate(data)
    model = BEARawRespons.model_validate(config.source_data)
    result: list[ParsedItems] = []
    skiped_value: list[str] = []
    len_skiped = 0

    # chek is code name is exists
    if data.meta.code_name is None:
        raise exc.BEAParserError("code_name is None, cannot filter SeriesCode")

    logger.debug(
        "Parsing Data, debug raw data to procesed %s Data, Sample: %s",
        len(model.BEAAPI.Results.Data),
        model.BEAAPI.Results.Data,
    )
    for item in model.BEAAPI.Results.Data:
        try:
            # skiping none target code_name
            if item.SeriesCode != config.meta.code_name:
                continue
            # build date key
            year, month = item.TimePeriod.split("M")
            date_key = f"{year}-{month.zfill(2)}-01"

            # filter none value
            if item.DataValue in ["", " ", "N/A", "-"]:
                logger.info("skiping value %s", item.DataValue)
                len_skiped += 1
                skiped_value.append(item.DataValue)
                continue
            # convert str -> Decimal
            values = Decimal(item.DataValue)
            result.append(
                ParsedItems(
                    date_key=date_key,
                    value=values,
                )
            )
        except (ValueError, TypeError) as e:
            logger.error("Skipping Parsing data: %s", e)
            continue
        except Exception as e:
            raise exc.BEAParserError(f"Parsing BEA Unknown Error {e}") from e
    if len_skiped > 0:
        logger.info("skipping value  %s, %s", len_skiped, skiped_value)

    logger.info("successfult parsing with %s data", len(result))

    return ParseResult(parse_result=result)
