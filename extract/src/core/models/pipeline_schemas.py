from pydantic import BaseModel
from typing import Any, Literal


class FinalresultFetcher(BaseModel):
    """Base Class Final Result ALL Fetcher"""

    source: Literal["bls", "fred", "bea"] | None = None
    fetch_result: Any


class FinalresultParse(BaseModel):
    """Base Class Final Result ALL Parse"""

    parse_result: dict[str, float]
