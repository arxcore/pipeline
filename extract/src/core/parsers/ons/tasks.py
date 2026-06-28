import logging
from typing import Any
from core.models.parsing_schemas import ParsedItems
from core.models.pipeline_schemas import FileResult, ParseResult
from core.parsers.ons.csv import parser_csv
from core.parsers.ons.excl.parse_excl import parser_excl

logger = logging.getLogger(__name__)


def route_task(metafile: list[FileResult]):
    """route task for excl and csv files
    Args:
        metafile (list[FileResult]): list of file result
    Returns:
        ParseResult: parse result
    """
    results: list[ParsedItems] = []
    errors: list[dict[str, Any]] = []
    for x in metafile:
        try:
            ext = x.file_ext.lower().strip() if x.file_ext else None
            if ext is None:
                logger.error("none type %s", x.file_ext)
                raise ValueError("Unknown file_ext type")

            elif ext == ".csv":
                task = parser_csv(x)
                if task is None:
                    raise ValueError

                results.extend(task)

            elif ext in [".xls", ".xlsx"]:
                task = parser_excl(x)
                if task is None:
                    raise ValueError
                results.extend(task)
            else:
                raise NotImplementedError(f"Unhandel {x.file_ext} file")
        except Exception as e:
            logger.error("Unexpected error %s", e)
            errors.append({"file_name": metafile[0].file_path.name, "eror": str(e)})

    logger.info("succesfuly process %s record, %s errors", len(results), len(errors))
    logger.debug("succesfuly process %s record, %s errors", results, errors)

    return ParseResult(parse_result=results)
