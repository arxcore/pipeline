from decimal import Decimal
import logging
import re
from typing import Any
import polars as pl
from core.models.parsing_schemas import ParsedItems
from core.models.pipeline_schemas import FileResult

logger = logging.getLogger(__name__)


def _find_codename_infile(file: pl.DataFrame, meta: FileResult):
    """
    Find the column and row index of the code name in the Excel file.

    Args:
        file (pl.DataFrame): The Excel file as a Polars DataFrame.
        meta (FileResult): The metadata of the file.

    Returns:
        tuple: The column and row index of the code name.

    Raises:
        ValueError: If the code name is not found in the file.
    """
    if not meta.code_name:
        raise ValueError("code_name is empty")

    cdid_pattern = re.compile(re.escape(meta.code_name))
    target_colum = None
    target_row = None

    logger.info("Searching for code_name: %s", meta.code_name)
    logger.debug("First 50 rows of the file: %s", file.head(50))

    # Iterate over the first 50 rows to find the code name
    for rows_idx, rows in enumerate(file.head(50).iter_rows()):
        for colums_idx, horiz_cell in enumerate(rows):
            # Check if the cell contains a string that matches the code name pattern
            if horiz_cell and isinstance(horiz_cell, str):
                if cdid_pattern.fullmatch(horiz_cell):
                    # Check if the matched string is the code name we're looking for
                    if meta.code_name == horiz_cell:
                        # Store the column and row index of the code name
                        target_row = rows_idx
                        target_colum = colums_idx
                        break
        # If we've found the code name, stop iterating
        if target_row is not None and target_colum is not None:
            break
    pattern_matces: list[Any] = []
    for rows_idx, rows in enumerate(file.head(100).iter_rows()):
        for column_idx, horiz_cell in enumerate(rows):
            if horiz_cell and isinstance(horiz_cell, str):
                pattern_matces.append((horiz_cell, rows_idx, column_idx))

    logger.debug("ALL Pattern Matches Found: %s", pattern_matces)
    if target_row is None or target_colum is None:
        raise ValueError(
            f"code_name {meta.code_name} not found. "
            f"Available codes in first 100 rows: {[match[0] for match in pattern_matces]}"
        )
    logger.info(
        "code_name %s found at Columns_Index %s, Row_Index %s",
        meta.code_name,
        target_colum,
        target_row,
    )
    return target_colum, target_row


def _slice_first_section(df: pl.DataFrame, date_type: str):
    pattern = {
        "datetime": r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$",
        "monthly": r"^\d{4}\s+[A-Za-z]{3}$",
        "quarterly": r"^\d{4}\s+Q\d$",
        "annual": r"^\d{4}$",
    }
    x = pattern[date_type]
    logger.info("Searching for date_type: %s", date_type)
    is_date = df.select(pl.nth(0).cast(pl.Utf8).str.contains(x).alias("is_date"))[
        "is_date"
    ]
    boundry = (
        pl.DataFrame({"is_date": is_date})
        .with_row_index()
        .with_columns(pl.col("is_date").cast(pl.Int32).cum_sum().alias("date_seen"))
        .filter((~pl.col("is_date")) & (pl.col("date_seen") > 0))
    )
    if len(boundry) > 0:
        cut_at = boundry["index"][0]
        logger.info("skiping section boundry at row %d", cut_at)
        return df.slice(0, cut_at)
    logger.info("No section boundry found")
    return df


def _date_parse_expres(date_type: str) -> pl.Expr:
    if date_type == "monthly":
        # 2xxx JAN, Jan-> jan
        lowered = pl.nth(0).cast(pl.Utf8).str.to_lowercase()

        # "2xxx "
        year_part = lowered.str.extract(r"^(\d{4} )", 0)

        # 2xxx Jan/JAN -> Jan
        month_part = lowered.str.extract(r"(\w{3})$", 1)

        # Jan JAN -> Jan
        month_title = month_part.str.slice(
            0, 1
        ).str.to_uppercase() + month_part.str.slice(1)

        return (
            (year_part + month_title)
            .str.strptime(dtype=pl.Datetime, format="%Y %b", strict=False)
            .dt.strftime("%Y-%m-%d")
        )

    elif date_type == "quarterly":
        raw = pl.nth(0).cast(pl.Utf8)
        year_part = raw.str.extract(r"^(\d{4})", 1)
        q_num = raw.str.extract(r"Q(\d)$", 1).cast(pl.Int32)
        month_num = ((q_num - 1) * 3 + 1).cast(pl.Utf8).str.zfill(2)
        return (
            (year_part + pl.lit("-") + month_num + pl.lit("-01"))
            .str.strptime(dtype=pl.Datetime, format="%Y-%m-%d", strict=False)
            .dt.strftime("%Y-%m-%d")
        )
    elif date_type == "annual":
        return (
            pl.nth(0)
            .cast(pl.Utf8)
            .str.strptime(dtype=pl.Datetime, format="%Y", strict=False)
            .dt.strftime("%Y-%m-%d")
        )

    elif date_type == "datetime":
        return (
            pl.nth(0)
            .cast(pl.Utf8)
            .str.strptime(dtype=pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False)
            .dt.strftime("%Y-%m-%d")
        )
    else:
        raise ValueError("Unknown Date Type %s", date_type)


def _detect_type_date(df: pl.DataFrame):
    """detect how the date is stored in files, not frequency"""
    date_col = df[df.columns[0]].cast(pl.Utf8)
    logger.info("Sample format date %s", date_col.head(20).to_list())
    if date_col.str.contains(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$").any():
        return "datetime"
    return "native"


def parser_excl(meta: FileResult):
    """
    Parse the Excel file and extract the data.

    Args:
        meta (FileResult): The metadata of the file.

    Returns:
        list: A list of ParsedItems objects containing the extracted data.

    Raises:
        Exception: If an error occurs during parsing.
    """
    logger.info(
        "code %s, type %s sheet_name %s", meta.code_name, meta.file_ext, meta.sheet_name
    )

    excel = pl.read_excel(
        source=meta.file_path,
        sheet_name=meta.sheet_name,
        engine="calamine",
        has_header=False,
        infer_schema_length=None,
    )
    # Find the column and row index of the code name
    find_codename = _find_codename_infile(excel, meta)
    colums: int
    row: int
    colums, row = find_codename
    try:
        logger.debug("Sample Original File %s", pl.DataFrame(excel.head(20)))

        # Extract the data from the Excel file
        # Take data from the row after the code name
        df_data = excel.slice(offset=row + 1)
        logger.info("row start at row %s", row + 1)

        storage = _detect_type_date(df_data)
        parse_type = "datetime" if storage == "datetime" else meta.freq
        # skip second section in midle file
        skip_sess = _slice_first_section(df_data, parse_type)

        # Create an alias table with the date and value columns
        df_result = skip_sess.select(
            [
                _date_parse_expres(parse_type).alias("date"),
                pl.nth(colums).alias("value"),
            ]
        )

        # Filter out rows with null values in the date and value columns
        df_filter = df_result.filter(
            pl.col("date").is_not_null(),
            pl.col("value").is_not_null(),
            pl.col("value") != "",
        )
        logger.info("Final Filtered excel data %s", pl.DataFrame(df_filter))
        # Convert the data to a list of ParsedItems objects
        return [
            ParsedItems(
                date_key=str(row["date"]),
                value=Decimal(str(row["value"])),
            )
            # Iterate over the rows in the filtered DataFrame
            for row in df_filter.iter_rows(named=True)
            if row["date"] and row["value"]
        ]

    except (ValueError, TypeError) as e:
        logger.error("Failed to parse date: %s", str(e))
        return None

    except Exception as e:
        logger.error("Failed to extract data for code %s: %s", meta.code_name, str(e))
        raise
