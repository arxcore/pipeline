from pydantic import BaseModel
from providers import BaseMetaModel


class FREDConfigModel(BaseMetaModel):
    pass


class Observation(BaseModel):
    date: str
    value: str


class FREDRawResponse(BaseModel):
    observations: list[Observation]
