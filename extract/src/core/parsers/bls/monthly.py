import logging
from core.models import FinalresultParse, FinalresultFetcher
from providers.bls.model import BLSRawResponsedata
from core.parsers.registry import register, Providers, Frequency
import monitoring.exc_models as exc

logger = logging.getLogger(__name__)


@register(Providers.bls, Frequency.monthly)
def parse_monthly_bls(data: FinalresultFetcher) -> FinalresultParse:
    """
    Docstring for parse_monthly_bls

    :param data: raw data bls api

    Return dict[str, float]
    """

    # Validation FinalresultFetcher
    RAW_DATA = BLSRawResponsedata.model_validate(data.fetch_result)

    parse_data: dict[str, float] = {}
    error: list[str] = []
    skip_value = 0

    logger.debug(
        "Parsing Data, debug raw data to procesed %s Data, Sample: %s",
        len(RAW_DATA.Results.series),
        RAW_DATA.Results.series,
    )

    try:
        for item in RAW_DATA.Results.series:
            for data_point in item.data:
                # target 2023-01: value

                year = data_point.year
                period = data_point.period

                # skip non-montly data
                if not period.startswith("M"):
                    logger.warning("skipping monthly data")
                    continue

                # # M01 convert to 01, etc..
                month = period[1:].zfill(2)

                # # build date key
                date_key = f"{year}-{month}-01"

                # # ambil value default 0
                str_value = data_point.value

                # # skip value = -, n/a etc..
                if str_value in ["-", "N/A", "NA", ""]:
                    skip_value += 1
                    error.append(str_value)
                    continue

                try:
                    # convert str -> float
                    value = float(str_value)

                    parse_data[date_key] = value

                except ValueError as e:
                    logger.error("Skipping Parsing data: %s", e)
                    # contoh: 300 valid, 1 error = skip 1 eror dan lanjut
                    continue

    except Exception as e:
        raise exc.BLSParserError(f"Parsing Monthly BLS Unknown Error {e}") from e

    logger.debug("monthly Parsing done %s data", len(parse_data))

    if skip_value > 0:
        logger.info("skipping value  %s -> %s", skip_value, error)

    return FinalresultParse(parse_result=parse_data)
