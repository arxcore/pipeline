from dataclasses import dataclass
from pathlib import Path
from pydantic import BaseModel, ConfigDict, Field
from typing import Any, TypeGuard

from core.models.parsing_schemas import ParsedItems
from providers.metamodel import BaseMetaModel


class FetchMeta(BaseMetaModel):
    country: str
    category: str
    indicator: str
    load_at: str
    checksum: str


class ApiResult(BaseModel):
    """Base Class Final Result apis Fetcher"""

    source_data: dict[str, Any]
    meta: FetchMeta


class FileResult(BaseMetaModel):
    file_path: Path
    file_ext: str | None = None
    country: str
    category: str
    indicator: str
    # source: str
    code_name: str | None = None
    etag: str | None = Field(default=None, alias="Etag")
    # freq: str
    # calc: str
    # optional


class ParseResult(BaseModel):
    """Base Class Final Result ALL Parse"""

    parse_result: list[ParsedItems]


class EtagLoad(BaseModel):
    """Validate after fetch etag from DB"""

    file_path: Path
    indicator: str
    code_name: str
    source: str
    etag: str | None = None


def is_file_result(
    data: list[FileResult] | list[ApiResult],
) -> TypeGuard[list[FileResult]]:
    return bool(data) and isinstance(data[0], FileResult)


# type hint type off all datas
# Fetchresult: TypeAlias = (
#    list[ApiResult] | list[FileResult] | tuple[list[FileResult], list[ApiResult]] | None
# )


@dataclass
class ApisRawResult:
    raw_respons: dict[str, Any]


class FilePathResult(BaseModel):
    model_config = ConfigDict(extra="ignore")
    path: Path
    etag: str | None = Field(None, alias="ETag")


@dataclass
class FetchBatchResult:
    file: list[FileResult]
    apis: list[ApiResult]
