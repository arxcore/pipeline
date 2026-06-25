from pathlib import Path
import polars as pl


def read_csv(path: Path, skip_rows: int = 0) -> pl.DataFrame:
    return pl.read_csv(path, skip_rows=skip_rows)


def read_excl_raw(path: Path, sheet: str | None = None) -> pl.DataFrame:
    return pl.read_excel(path, sheet_name=sheet, engine="calamine")
