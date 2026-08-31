""" "Miscellaneous Activity"""

from functools import lru_cache

import polars as pl

from data_source.make_dataset import load_sheet as scan_google_sheet
from data_source.sheet_ids import ALL_CCCS_DATA_SHEET_NAME, MISC_SHEET_ID
from type_casting import CURRENT_YEAR, DayName


def _to_f64(column: str) -> pl.Expr:
    """Normalize mixed/blank numeric cells to Float64 with 0 fallback."""
    return (
        pl.col(column)
        .cast(pl.Utf8)
        .str.strip_chars()
        .replace("", None)
        .cast(pl.Float64, strict=False)
        .fill_null(0.0)
    )


# Miscellaneous Main Sheet clean up
@lru_cache(maxsize=1)
def miscellaneous_lf() -> pl.LazyFrame:
    """Miscellaneous main sheet"""
    return (
        scan_google_sheet(sheet_id=MISC_SHEET_ID, sheet_name=ALL_CCCS_DATA_SHEET_NAME)
        .filter(pl.col("date").dt.year().eq(CURRENT_YEAR))
        .select(
            pl.col("day_name").cast(dtype=DayName.enum_dtype()),
            pl.col("date"),
            pl.col("movement_type"),
            pl.col("customer"),
            pl.col("origin"),
            pl.col("vessel"),
            pl.col("storage_type"),
            pl.col("service").alias("operation_type"),
            _to_f64("total_tonnage").alias("total_tonnage"),
            pl.col("bins_in").str.replace("", "0").cast(pl.Int64),
            pl.col("bins_out").str.strip_chars("-").replace("", "0").cast(pl.Int64) * -1,
            _to_f64("static_loader").alias("static_loader"),
            _to_f64("overtime_tonnage").alias("overtime_tonnage"),
        )
    )
