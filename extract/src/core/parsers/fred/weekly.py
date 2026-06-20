import logging
from core.parsers.registry import register, Providers, Frequency
from core.models import ParseResult, ApiResult

logger = logging.getLogger(__name__)


@register(Providers.fred, Frequency.weekly)
def parse_monthly_fred(data: ApiResult) -> ParseResult:
    logger.info("-_")
    raise NotImplementedError("Fred Weekly parser not implementation")
