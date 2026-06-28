from pydantic import BaseModel, ConfigDict
from providers import BaseMetaModel


class BEAConfigModel(BaseMetaModel):
    dataset: str
    table: str | None = None
    line_number: str | None = None
    # method: Literal[
    #     "GetDatasetList", "GetParameterList", "GetParameterValues", "GetData"
    # ]
    # datasetname: str
    # areaorcountry: Literal["AllCountries"]
    # format: str
    # ParameterName: Optional[Literal["Indicator"]] | None = None


class BEAField(BaseModel):
    model_config = ConfigDict(extra="ignore")

    SeriesCode: str | None = None
    TimePeriod: str
    DataValue: str
    NoteRef: str | None = None


class BEANotes(BaseModel):
    NoteRef: str | None = None
    NoteText: str | None = None


class BEAResult(BaseModel):
    Data: list[BEAField]
    Notes: list[BEANotes]


class BEAapi(BaseModel):
    Results: BEAResult


class BEARawRespons(BaseModel):
    BEAAPI: BEAapi
