from typing import Callable
from core.models import ApiResult, ParseResult
from enum import Enum

# CONSTANT
FUNCTION = Callable[[ApiResult], ParseResult]
PARSE_REGISTER: dict[str, dict[str, FUNCTION]] = {}


class Providers(str, Enum):
    bls = "bls"
    fred = "fred"
    bea = "bea"
    ons = "ons"


class Frequency(str, Enum):
    m = "M"
    weekly = "weekly"
    monthly = "monthly"
    quarterly = "quarterly"
    qsa = "QSA"  # Quarterly Seasonally Adjusted
    annual = "annual"


# registry Parse
def register(providers: Providers, freq: Frequency):
    def wraper(func: FUNCTION):
        PARSE_REGISTER.setdefault(providers, {})[freq] = func
        return func

    return wraper
