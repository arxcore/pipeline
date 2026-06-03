from typing import Callable
from core.models import FinalresultFetcher, FinalresultParse
from enum import Enum

# CONSTANT
FUNCTION = Callable[[FinalresultFetcher], FinalresultParse]
PARSE_REGISTER: dict[str, dict[str, FUNCTION]] = {}


class Providers(str, Enum):
    bls = "bls"
    fred = "fred"
    bea = "bea"


class Frequency(str, Enum):
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
