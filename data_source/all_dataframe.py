"""All the dataframe call and clean up"""

import polars as pl
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import (
    MISC_SHEET_ID,
    ALL_CCCS_DATA_SHEET,
    CROSS_STUFFING_SHEET,
    BY_CATCH_SHEET,
    CCCS_STUFFING_SHEET,
)
from type_casting.dates import CURRENT_YEAR, Days, DayName

from type_casting.containers import containers_enum


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
def miscellaneous() -> pl.LazyFrame:
    """Miscellaneous main sheet"""
    return (
        load_gsheet_data(sheet_id=MISC_SHEET_ID, sheet_name=ALL_CCCS_DATA_SHEET)
        .unwrap()
        .filter(pl.col("date").dt.year().ge(CURRENT_YEAR - 1))
        .select(
            pl.col("day").cast(dtype=DayName.enum_dtype()),
            pl.col("date"),
            pl.col("movement_type"),
            pl.col("customer"),
            pl.col("origin"),
            pl.col("vessel"),
            pl.col("storage_type"),
            pl.col("operation_type"),
            _to_f64("total_tonnage").alias("total_tonnage"),
            pl.col("bins_in").str.replace("", "0").cast(pl.Int64),
            pl.col("bins_out").str.strip_chars("-").replace("", "0").cast(pl.Int64)
            * -1,
            _to_f64("static_loader").alias("static_loader"),
            _to_f64("overtime_tonnage").alias("overtime_tonnage"),
        )
    )


def cross_stuffing() -> pl.LazyFrame:
    """Cross stuffing sheet"""
    return (
        load_gsheet_data(MISC_SHEET_ID, CROSS_STUFFING_SHEET)
        .unwrap()
        # .filter(pl.col("day").str.replace("", "x").ne("x"))
        .with_columns(storage_type=pl.lit("Dry", dtype=pl.Utf8))
        .filter(pl.col("date").dt.year().ge(CURRENT_YEAR - 1))
        .select(
            pl.col("day").cast(dtype=DayName.enum_dtype()),
            pl.col("vessel_client"),
            pl.col("date"),
            pl.col("origin"),  # .cast(dtype=containers_enum)
            pl.col("destination"),
            pl.col("start_time"),
            pl.col("end_time"),
            pl.col("storage_type"),
            _to_f64("total_tonnage").round(3).alias("total_tonnage"),
            _to_f64("overtime_tonnage").round(3).alias("overtime_tonnage"),
            pl.col("is_origin_empty"),
            pl.col("service").alias("Service"),
            pl.col("invoiced"),
        )
    )


def by_catch_transfer() -> pl.LazyFrame:
    """by catch transfer sheet"""
    return (
        load_gsheet_data(MISC_SHEET_ID, BY_CATCH_SHEET)
        .unwrap()
        .filter(pl.col("date").dt.year().ge(CURRENT_YEAR - 1))
        .with_columns(day=pl.col("date").days.add_day_name())
        .select(
            pl.col("day"),
            pl.col("date"),
            pl.col("movement_type"),
            pl.col("customer"),
            pl.col("vessel"),
            pl.col("service").alias("operation_type"),
            pl.col("storage_type"),
            _to_f64("total_tonnage").round(3).alias("total_tonnage"),
            _to_f64("overtime_tonnage").round(3).alias("overtime_tonnage"),
        )
    )


def cccs_container_stuffing() -> pl.LazyFrame:
    """CCCS container stuffing dataframe clean up"""
    return (
        load_gsheet_data(MISC_SHEET_ID, CCCS_STUFFING_SHEET)
        .and_then(lambda x: x.filter(pl.col("date").dt.year().eq(CURRENT_YEAR)))
        .with_columns(storage_type=pl.lit("Dry", dtype=pl.Utf8))
        .select(
            pl.col("date").days.add_day_name(),
            pl.col("date"),
            pl.col("container_number").cast(dtype=containers_enum),
            pl.col("customer"),
            pl.col("service").alias("Service"),
            pl.col("storage_type"),
            _to_f64("total_tonnage").alias("total_tonnage"),
            _to_f64("overtime_tonnage").alias("overtime_tonnage"),
            pl.col("invoiced"),
        )
    )
