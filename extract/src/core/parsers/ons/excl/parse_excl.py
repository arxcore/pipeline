from datetime import datetime
from decimal import Decimal
import logging
import re
import polars as pl
from core.models.parsing_schemas import ParsedItems
from core.models.pipeline_schemas import FileResult

logger = logging.getLogger(__name__)


def _find_codename_infile(file: pl.DataFrame, meta: FileResult):
    cdid_pattern = re.compile(r"^[A-Z]{3}\d{1}$")
    target_colum = None
    target_row = None

    for rows_idx, rows in enumerate(file.head(50).iter_rows()):
        for colums_idx, horiz_cell in enumerate(rows):
            if horiz_cell and isinstance(horiz_cell, str):
                if cdid_pattern.match(horiz_cell):
                    if meta.code_name == horiz_cell:
                        target_row = rows_idx
                        target_colum = colums_idx
                        break
        if target_row is not None and target_colum is not None:
            break
    if target_row is None or target_colum is None:
        raise ValueError("code_name %s not found", meta.code_name)
    logger.info(
        "code_name %s found at Columns_Index %s, Row_Index %s",
        meta.code_name,
        target_colum,
        target_row,
    )
    return target_colum, target_row


def parser_excl(meta: FileResult):
    logger.info("code %s, type %s", meta.code_name, meta.file_ext)

    excel = pl.read_excel(
        source=meta.file_path,
        sheet_name="3. AWE Regular Pay",
        engine="calamine",
        has_header=False,
        infer_schema_length=None,
    )
    find_codename = _find_codename_infile(excel, meta)
    colums: int
    row: int
    colums, row = find_codename
    try:
        logger.debug("sample data %s", excel.head(20))

        # take data
        df_data = excel.slice(offset=row + 1)

        # create alias table
        df_result = df_data.select(
            [
                pl.nth(0).alias("date"),
                pl.nth(colums).alias("value"),
            ]
        )

        # filter null value date and value colomns
        df_filter = df_result.filter(
            pl.col("date").is_not_null(),
            pl.col("value").is_not_null(),
        )
        return [
            ParsedItems(
                date_key=datetime.strptime(
                    str(row["date"]), "%Y-%m-%d %H:%M:%S"
                ).strftime("%Y-%m-%d"),
                value=Decimal(str(row["value"])),
            )
            for row in df_filter.iter_rows(named=True)
            # Additional safety check
            if row["date"] and row["value"]
        ]

    except (ValueError, TypeError) as e:
        logger.error("Failed to parse date: %s", str(e))
        return None

    except Exception as e:
        logger.error("Failed to extract data for code %s: %s", meta.code_name, str(e))
        raise
