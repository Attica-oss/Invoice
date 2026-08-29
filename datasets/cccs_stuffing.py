"""CCCS Container Stuffing dataset with per-service overtime pricing."""

from __future__ import annotations

from functools import lru_cache

import polars as pl
from data_source.make_dataset import load_sheet as scan_google_sheet

from datasets.price import get_price
from utils import CURRENT_YEAR, SPECIAL_DAYS, Days, OvertimePerc, containers_enum
from utils.config import CCCS_STUFFING_SHEET_NAME, MISC_SHEET_ID

CCCS_STUFFING_SERVICES: tuple[str, ...] = (
    "Shore Crane & Fishloader",
    "Shore Crane & Fishloader (by catch)",
    "Static Loader",
    "Container Stuffing by Hand",
    "Container Stuffing with Forklift",
)


def _to_f64(column: str) -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.Utf8)
        .str.strip_chars()
        .replace("", None)
        .cast(pl.Float64, strict=False)
        .fill_null(0.0)
    )


@lru_cache(maxsize=1)
def cccs_stuffing_price() -> pl.LazyFrame:
    return get_price(list(CCCS_STUFFING_SERVICES)).sort("service", "date").lazy()


@lru_cache(maxsize=1)
def cccs_stuffing() -> pl.LazyFrame:
    return (
        scan_google_sheet(sheet_id=MISC_SHEET_ID, sheet_name=CCCS_STUFFING_SHEET_NAME)
        .filter(pl.col("date").dt.year().eq(CURRENT_YEAR))
        .select(
            pl.col("date").days.add_day_name(),
            pl.col("date"),
            pl.col("container_number").cast(containers_enum),
            pl.col("customer"),
            pl.col("service"),
            pl.lit("Dry", dtype=pl.Utf8).alias("storage_type"),
            _to_f64("total_tonnage").alias("total_tonnage"),
            _to_f64("overtime_tonnage").alias("overtime_tonnage"),
            pl.col("invoiced"),
        )
        .sort("service", "date")
        .join_asof(
            cccs_stuffing_price(),
            by="service",
            on="date",
            strategy="backward",
        )
        .with_columns(normal_tonnage=pl.col("total_tonnage") - pl.col("overtime_tonnage"))
        .with_columns(
            total_price=pl.when(pl.col("day_name").cast(pl.Utf8).is_in(SPECIAL_DAYS))
            .then(
                (pl.col("normal_tonnage") * OvertimePerc.overtime_150 * pl.col("unit_price"))
                + (pl.col("overtime_tonnage") * OvertimePerc.overtime_200 * pl.col("unit_price"))
            )
            .otherwise(
                (pl.col("normal_tonnage") * OvertimePerc.normal_hour * pl.col("unit_price"))
                + (pl.col("overtime_tonnage") * OvertimePerc.overtime_150 * pl.col("unit_price"))
            )
        )
        .select(pl.all().exclude("normal_tonnage"))
    )


CCCS_STUFFING_DATASET = cccs_stuffing()
