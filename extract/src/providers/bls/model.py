from pydantic import BaseModel
from providers import BaseMetaModel


# Config Metadata Model
class BLSConfigModel(BaseMetaModel):
    pass


# BLS RAW DATA MODEL
class BLSFootnotes(BaseModel):
    code: str | None = None
    text: str | None = None


class BLSRawData(BaseModel):
    year: str
    period: str
    periodName: str
    latest: str | None = None
    value: str
    footnotes: list[BLSFootnotes]


class BLSResult(BaseModel):
    seriesID: str
    data: list[BLSRawData]


class BLSSeries(BaseModel):
    series: list[BLSResult]


class BLSRawResponsedata(BaseModel):
    status: str
    responseTime: int
    message: list[str]
    Results: BLSSeries
