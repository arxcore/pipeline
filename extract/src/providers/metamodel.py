from pydantic import BaseModel
from typing import Literal


class BaseMetaModel(BaseModel):
    code_name: str | None = None
    source: Literal["bls", "fred", "bea", "ons"]
    calc: Literal["net", "raw", "wow", "mom", "3m_avg", "yoy", "qoq"]
    freq: Literal["weekly", "monthly", "M", "QSA", "quarterly", "annual"]
    start_year: int | None = None
    start_month: int | None = None
    unit: str | None = None
    sheet_name: str | None = None
    description: str
