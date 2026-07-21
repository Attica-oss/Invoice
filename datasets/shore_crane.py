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

    is_special = pl.col("day_name").is_in(SPECIAL_DAYS)
    is_after = pl.col("date") >= CUT_OFF_DATE

    normal_mult = (
        pl.when(is_special & is_after)
        .then(1.6)
        .when(is_special)
        .then(OvertimePerc.overtime_150)
        .otherwise(OvertimePerc.normal_hour)
    )
    ot_mult = (
        pl.when(is_special & is_after)
        .then(2.1)
        .when(is_special)
        .then(OvertimePerc.overtime_200)
        .when(is_after)
        .then(1.6)
        .otherwise(OvertimePerc.overtime_150)
    )

    return (
        scan_google_sheet(
            sheet_id=TRANSPORT_SHEET_ID, sheet_name=SHORE_CRANE_SHEET_NAME
        )
        .filter(pl.col("date").dt.year().eq(CURRENT_YEAR))
        .select(
            pl.col("day").cast(DayName.enum_dtype()).alias("day_name"),
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
        .with_columns(normal_hours=pl.col("hours") - pl.col("overtime_hours"))
        .with_columns(
            unit_price=(pl.lit(price) * normal_mult).round(3),
            total_price=(
                pl.col("normal_hours") * price * normal_mult
                + pl.col("overtime_hours") * price * ot_mult
            ).round(3),
        )
        .select(
            "day_name",
            "date",
            "start_time",
            "end_time",
            "hours",
            "overtime_hours",
            "customer",
            "location",
            "operation_type",
            "invoiced_to",
            "unit_price",
            "total_price",
        )
        .sort("date", "start_time")
    )


SHORE_CRANE_DATASET = shore_crane()
