from pydantic import BaseModel
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
    TimePeriod: str
    DataValue: str


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
