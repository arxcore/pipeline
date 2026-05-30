from pydantic import BaseModel
from typing import Literal


class BaseMetaModel(BaseModel):
    code_name: str
    source: Literal["bls", "fred", "bea"]
    calc: Literal["net", "raw", "wow", "mom", "yoy", "qoq"]
    freq: Literal["weekly", "monthly", "QSA", "quarterly", "annual"]
    start_year: int
    start_month: int
    unit: str
    description: str
