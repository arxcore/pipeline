from pathlib import Path
from pydantic import ConfigDict, Field, BaseModel
from providers.metamodel import BaseMetaModel


class ONSConfigModel(BaseMetaModel):
    url: str


class OnsResult(BaseModel):
    model_config = ConfigDict(extra="ignore")
    path: Path
    etag: str | None = Field(None, alias="ETag")
