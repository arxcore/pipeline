from datetime import datetime
from decimal import Decimal
import logging
import polars as pl
from core.models.parsing_schemas import ParsedItems
from core.models.pipeline_schemas import FileResult

logger = logging.getLogger(__name__)


def period_to_iso(period: str, frequency: str):
    if frequency == "monthly":
        return datetime.strptime(period, "%Y %b").strftime("%Y-%m-%d")
    elif frequency == "quarterly":
        year, q = period.split("Q")
        month = (int(q) - 1) * 3 + 1
        return f"{year.strip()}-{month:02d}-01"
    elif frequency == "annual":
        return f"{period}-01-01"
    else:
        logger.error("unhandel frequency %s", frequency)
        raise NotImplementedError


def parser_csv(meta: FileResult):
    try:
        with pl.Config(tbl_rows=-1, tbl_cols=-1, fmt_str_lengths=1000):
            csv = pl.read_csv(
                source=meta.file_path,
                skip_rows=8,
                has_header=False,
                new_columns=["period", "value"],
            )
            # NOTE: debug
            # test = csv.with_columns(
            #    is_match=pl.col("period").str.contains(r"^\d{4}\s+[A-Z]{3}$")
            # )
            # logger.info(test.filter(pl.col("is_match")))
            frequency = meta.freq.strip().lower()
            patterns = {
                "monthly": r"^\d{4}\s+[A-Z]{3}$",
                "quarterly": r"^\d{4}\s+Q[1-4]$",
                "annual": r"^\d{4}$",
            }
            pattern = patterns.get(frequency)
            if pattern is None:
                logger.error("frequency %s not Implemented regex patterns", frequency)
                return None

            filters = csv.filter(pl.col("period").str.contains(pattern))
            logger.debug(filters)
            if filters.shape[0] == 0:
                logger.info("No data was found", filter)
                return None
            logger.info(
                "%s, %d rows match, frequency %s",
                meta.indicator,
                filters.shape[0],
                frequency,
            )
            return [
                ParsedItems(
                    date_key=period_to_iso(row["period"], frequency),
                    value=Decimal(str(row["value"])),
                )
                for row in filters.iter_rows(named=True)
                # Additional safety check
                if row["period"] and row["value"]
            ]
    except (ValueError, TypeError) as e:
        logger.error("Failed to parse date: %s", str(e))
        return None
    except Exception as e:
        logger.error("Failed to extract data for code %s: %s", meta.code_name, str(e))
        raise
