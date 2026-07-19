"""Shore crane rental dataset with overtime-adjusted pricing."""

from __future__ import annotations

from datetime import date
from functools import lru_cache

import polars as pl
from scan_google_sheet import scan_google_sheet

from datasets.price import unit_price
from utils import (
    CURRENT_YEAR,
    SPECIAL_DAYS,
    DayName,
    OvertimePerc,
)
from utils.config import SHORE_CRANE_SHEET_NAME, TRANSPORT_SHEET_ID

# Shore crane OT rates changed on this date: special-day rate moved to 1.6x,
# normal-day OT moved from 1.5x to 1.6x.
CUT_OFF_DATE: date = date(2026, 3, 1)


@lru_cache(maxsize=1)
def shore_crane() -> pl.LazyFrame:
    price = unit_price("Shore Crane")

    return (
        scan_google_sheet(sheet_id=TRANSPORT_SHEET_ID, sheet_name=SHORE_CRANE_SHEET_NAME)
        .filter(pl.col("date").dt.year().eq(CURRENT_YEAR))
        .select(
            pl.col("day").cast(DayName.enum_dtype()),
            pl.col("date"),
            pl.col("start_time"),
            pl.col("end_time"),
            pl.col("hours").dt.hour(),
            pl.col("overtime_hours").dt.hour(),
            pl.col("customer").cast(pl.Utf8),
            pl.col("location").cast(pl.Utf8),
            pl.col("operation_type"),
            pl.col("invoiced_to"),
        )
        .with_columns(
            unit_price=pl.when(
                pl.col("day").is_in(SPECIAL_DAYS) & (pl.col("date") >= CUT_OFF_DATE)
            )
            .then(price * 1.6)
            .when(pl.col("day").is_in(SPECIAL_DAYS))
            .then(price * OvertimePerc.overtime_150)
            .otherwise(price)
            .round(3)
        )
        .with_columns(normal_hours=pl.col("hours") - pl.col("overtime_hours"))
        .with_columns(
            total_price=pl.when(
                pl.col("day").is_in(SPECIAL_DAYS) & (pl.col("date") >= CUT_OFF_DATE)
            )
            .then(
                (
                    pl.col("normal_hours").cast(pl.Decimal(precision=3))
                    * pl.lit(price, dtype=pl.Decimal(precision=3))
                    * pl.lit(1.6, dtype=pl.Decimal(precision=3))
                )
                + (
                    pl.col("overtime_hours").cast(pl.Decimal(precision=3))
                    * pl.lit(price, dtype=pl.Decimal(precision=3))
                    * pl.lit(2.1, dtype=pl.Decimal(precision=3))
                )
            )
            .when(pl.col("day").is_in(SPECIAL_DAYS))
            .then(
                (
                    pl.col("normal_hours").cast(pl.Decimal(precision=3))
                    * pl.lit(price, dtype=pl.Decimal(precision=3))
                    * pl.lit(OvertimePerc.overtime_150, dtype=pl.Decimal(precision=3))
                )
                + (
                    pl.col("overtime_hours").cast(pl.Decimal(precision=3))
                    * pl.lit(price, dtype=pl.Decimal(precision=3))
                    * pl.lit(OvertimePerc.overtime_200, dtype=pl.Decimal(precision=3))
                )
            )
            .when(pl.col("date") >= CUT_OFF_DATE)
            .then(
                (
                    pl.col("normal_hours").cast(pl.Decimal(precision=3))
                    * pl.lit(price, dtype=pl.Decimal(precision=3))
                    * pl.lit(OvertimePerc.normal_hour, dtype=pl.Decimal(precision=3))
                )
                + (
                    pl.col("overtime_hours").cast(pl.Decimal(precision=3))
                    * pl.lit(price, dtype=pl.Decimal(precision=3))
                    * pl.lit(1.6, dtype=pl.Decimal(precision=3))
                )
            )
            .otherwise(
                (
                    pl.col("normal_hours").cast(pl.Decimal(precision=3))
                    * pl.lit(price, dtype=pl.Decimal(precision=3))
                    * pl.lit(OvertimePerc.normal_hour, dtype=pl.Decimal(precision=3))
                )
                + (
                    pl.col("overtime_hours").cast(pl.Decimal(precision=3))
                    * pl.lit(price, dtype=pl.Decimal(precision=3))
                    * pl.lit(OvertimePerc.overtime_150, dtype=pl.Decimal(precision=3))
                )
            )
            .round(3)
        )
        .select(
            pl.col("day").alias("day_name"),
            pl.col("date"),
            pl.col("start_time"),
            pl.col("end_time"),
            pl.col("hours"),
            pl.col("overtime_hours"),
            pl.col("customer"),
            pl.col("location"),
            pl.col("operation_type"),
            pl.col("invoiced_to"),
            pl.col("unit_price"),
            pl.col("total_price"),
        )
        .sort("date", "start_time")
    )


SHORE_CRANE_DATASET = shore_crane()
