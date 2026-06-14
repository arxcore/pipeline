from decimal import Decimal
from typing import Any
from pydantic import BaseModel


class ParsedItems(BaseModel):
    date_key: str
    value: Decimal
    footnotes: list[Any] | None = None
