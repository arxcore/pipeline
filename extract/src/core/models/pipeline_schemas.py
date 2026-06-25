from pathlib import Path
from pydantic import BaseModel
from typing import Any, TypeAlias, TypeGuard

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
    # freq: str
    # calc: str
    # optional


class ParseResult(BaseModel):
    """Base Class Final Result ALL Parse"""

    parse_result: list[ParsedItems]


def is_file_result(
    data: list[FileResult] | list[ApiResult],
) -> TypeGuard[list[FileResult]]:
    return bool(data) and isinstance(data[0], FileResult)


# type hint type off all datas
Fetchresult: TypeAlias = (
    list[ApiResult] | list[FileResult] | tuple[list[FileResult], list[ApiResult]] | None
)
