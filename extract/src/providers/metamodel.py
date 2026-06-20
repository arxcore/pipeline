from pydantic import BaseModel
from typing import Literal


class BaseMetaModel(BaseModel):
    code_name: str | None = None
    source: Literal["bls", "fred", "bea", "ons"]
    calc: Literal["net", "raw", "wow", "mom", "yoy", "qoq"]
    freq: Literal["weekly", "monthly", "M", "QSA", "quarterly", "annual"]
    start_year: int
    start_month: int
    unit: str | None = None
    description: str
