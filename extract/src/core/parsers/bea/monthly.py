import logging
from core.models import ApiResult
from core.models.pipeline_schemas import ParseResult
from core.parsers.registry import Frequency, Providers, register

logger = logging.getLogger(__name__)


@register(Providers.bea, Frequency.m)
def parse_monthly_bea(data: ApiResult) -> ParseResult:
    """Parse data monthly BEA return ParseResult"""
    # TODO:
    # implemented bea monthly parsing with core pce mom respons > exported_data/pce
    logger.warning("Monthly BEA Parsing Not Implemented..Skiping")
