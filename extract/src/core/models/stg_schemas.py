from datetime import date, datetime
from pydantic import BaseModel


class StagingItems(BaseModel):
    date: date
    year: int
    source: str
    code: str
    indicator: str
    country: str
    category: str
    value: float
    frequency: str
    method: str
    unit: str
    footnotes_note: str | None = None
    processed: datetime
    description: str


class StagingData(BaseModel):
    """Staging Data Model for Processed Indicators"""

    staging_result: list[StagingItems]
