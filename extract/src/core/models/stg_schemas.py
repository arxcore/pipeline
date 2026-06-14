from datetime import date, datetime
from decimal import Decimal
from pydantic import BaseModel
from typing import Any


class StagingItems(BaseModel):
    date: date
    year: int
    source: str
    code: str
    indicator: str
    country: str
    category: str
    value: Decimal
    frequency: str
    method: str
    unit: str
    footnotes_note: list[Any] | None = None
    processed: datetime
    description: str


class StagingData(BaseModel):
    """Staging Data Model for Processed Indicators"""

    staging_result: list[StagingItems]
