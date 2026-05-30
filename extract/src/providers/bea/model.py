from pydantic import BaseModel
from providers import BaseMetaModel


class BEAConfigModel(BaseMetaModel):
    pass
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


class BEAResult(BaseModel):
    Data: list[BEAField]


class BEAapi(BaseModel):
    Results: BEAResult


class BEARawRespons(BaseModel):
    BEAAPI: BEAapi
