from pydantic import BaseModel
from typing import Any, Literal


class FinalresultFetcher(BaseModel):
    """Base Class Final Result ALL Fetcher"""

    source: Literal["bls", "fred", "bea"] | None = None
    fetch_result: Any
