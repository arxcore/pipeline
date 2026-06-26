"""
This module provides functionality to parse CSV files for the ONS (Office for National Statistics) data pipeline.
It includes utilities to convert period strings to ISO date format and parse CSV files into structured data.
"""

from datetime import datetime
from decimal import Decimal
import logging
import polars as pl
from core.models.parsing_schemas import ParsedItems
from core.models.pipeline_schemas import FileResult

logger = logging.getLogger(__name__)


def period_to_iso(period: str, frequency: str):
    """
    Convert a period string to an ISO formatted date string based on the frequency.

    Args:
        period (str): The period string to convert (e.g., "2023 Jan", "2023 Q1", "2023").
        frequency (str): The frequency of the period (e.g., "monthly", "quarterly", "annual").

    Returns:
        str: ISO formatted date string (e.g., "2023-01-01").

    Raises:
        NotImplementedError: If the frequency is not supported.
    """
    if frequency == "monthly":
        # Handle monthly frequency (e.g., "2023 Jan" -> "2023-01-01")
        return datetime.strptime(period, "%Y %b").strftime("%Y-%m-%d")
    elif frequency == "quarterly":
        # Handle quarterly frequency (e.g., "2023 Q1" -> "2023-01-01")
        year, q = period.split("Q")
        month = (int(q) - 1) * 3 + 1
        return f"{year.strip()}-{month:02d}-01"
    elif frequency == "annual":
        # Handle annual frequency (e.g., "2023" -> "2023-01-01")
        return f"{period}-01-01"
    else:
        # Log error for unsupported frequency
        logger.error("unhandel frequency %s", frequency)
        raise NotImplementedError


def parser_csv(meta: FileResult):
    """
    Parse a CSV file based on the provided metadata and return structured data.

    Args:
        meta (FileResult): Metadata containing file path, frequency, and other details.

    Returns:
        list[ParsedItems] | None: List of parsed items or None if parsing fails.

    Raises:
        Exception: If an unexpected error occurs during parsing.
    """
    try:
        # Configure Polars to display all rows and columns for debugging purposes
        with pl.Config(tbl_rows=-1, tbl_cols=-1, fmt_str_lengths=1000):
            # Read CSV file with specific configurations
            csv = pl.read_csv(
                source=meta.file_path,
                # Skip the first 8 rows as they contain metadata or headers
                skip_rows=8,
                # The CSV file does not have a header row
                has_header=False,
                # Define new column names for the parsed data
                new_columns=["period", "value"],
            )
            # NOTE: debug
            # test = csv.with_columns(
            #    is_match=pl.col("period").str.contains(r"^\d{4}\s+[A-Z]{3}$")
            # )
            # logger.info(test.filter(pl.col("is_match"))
            frequency = meta.freq.strip().lower()
            # Define regex patterns for validating period strings based on frequency
            patterns = {
                # Pattern for monthly periods (e.g., "2023 Jan")
                "monthly": r"^\d{4}\s+[A-Z]{3}$",
                # Pattern for quarterly periods (e.g., "2023 Q1")
                "quarterly": r"^\d{4}\s+Q[1-4]$",
                # Pattern for annual periods (e.g., "2023")
                "annual": r"^\d{4}$",
            }
            # Retrieve the regex pattern based on the frequency
            pattern = patterns.get(frequency)
            # Log error if the frequency is not supported
            if pattern is None:
                logger.error("frequency %s not Implemented regex patterns", frequency)
                return None

            # Filter rows where the period column matches the expected pattern
            filters = csv.filter(pl.col("period").str.contains(pattern))
            # Log the filtered data for debugging purposes
            logger.debug("Filtered data %s", pl.DataFrame(filters))
            # Check if no rows match the filter criteria
            if filters.shape[0] == 0:
                # Log information about the absence of matching data
                logger.info("No data was found", filter)
                return None
            # Log the number of rows that match the filter criteria
            logger.info(
                "%s, %d rows match, frequency %s",
                meta.indicator,
                filters.shape[0],
                frequency,
            )
            # Convert each row into a ParsedItems object
            return [
                ParsedItems(
                    # Convert the period string to an ISO date string
                    date_key=period_to_iso(row["period"], frequency),
                    # Convert the value to a Decimal for precision
                    value=Decimal(str(row["value"])),
                )
                for row in filters.iter_rows(named=True)
                # Ensure that both period and value are non-empty
                if row["period"] and row["value"]
            ]
    except (ValueError, TypeError) as e:
        # Handle ValueError or TypeError exceptions during parsing
        logger.error("Failed to parse date: %s", str(e))
        return None
    except Exception as e:
        # Log error for unexpected exceptions during parsing
        logger.error("Failed to extract data for code %s: %s", meta.code_name, str(e))
        raise
