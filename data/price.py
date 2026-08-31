"""Pricing Module"""

from __future__ import annotations

from functools import lru_cache

import polars as pl

from data_source.make_dataset import load_sheet as scan_google_sheet
from data_source.sheet_ids import BC_ITEMS_SHEET_NAME, MASTER_ID, PRICE_SHEET_NAME
from type_casting import PolarsEnum

# Date formatting
DATE_FMT: str = "%d/%m/%Y"


class ServiceType(PolarsEnum):
    """Types of service"""

    berth_dues = "Berth Dues"
    bin_dispatch = "Bin Dispatch"
    by_catch = "By Catch"
    cold_store = "CCCS"
    cold_store_stuffing = "Cold Store Stuffing"
    cross_stuffing = "Cross Stuffing"
    depot = "Depot"
    electricity = "Electricity"
    vessel_unloading = "Net List"
    pre_trip_inspection = "PTI"
    salt = "Salt"
    stevedoring = "Stevedoring"
    transfer = "Transfer"


@lru_cache(maxsize=1, typed=True)
def price_table() -> pl.LazyFrame:
    """
    Load + clean the price sheet once (small table → keep in memory).
    """
    lf: pl.LazyFrame = scan_google_sheet(
        sheet_id=MASTER_ID, sheet_name=PRICE_SHEET_NAME, parse_dates=True
    )

    # A blank EndingDate marks the currently-active price. The 0.3.0 Rust
    # parser types an all-blank/date-like column as Date (blanks -> null),
    # where the old pure-Python parser left it Utf8 (blanks -> ""). Cast to
    # Utf8 first so the "is it blank" test works whichever way it inferred.
    ending_blank = (
        pl.col("EndingDate").cast(pl.Utf8, strict=False).replace("", None).is_null()
    )
    return lf.filter(ending_blank).select(
        pl.col("Service").alias("service"),
        pl.col("Class").alias("service_type"),
        pl.col("StartingDate").alias("date"),
        pl.col("Price").alias("unit_price").cast(pl.Float64),
    )


def get_price_by_type(service_type: ServiceType) -> pl.LazyFrame:
    """Filter the price table by ServiceType"""
    return price_table().filter(pl.col("service_type").eq(service_type))


def get_price(services: list[str] | None = None) -> pl.DataFrame:
    """Get the price table, optionally filtered by a list of services."""
    df = price_table()
    if services is None:
        return df.collect()
    return df.filter(pl.col("service").is_in(services)).collect()


def bc_items() -> pl.LazyFrame:
    """Load + clean the BC items sheet once (small table → keep in memory)."""
    lf: pl.LazyFrame = scan_google_sheet(sheet_id=MASTER_ID, sheet_name=BC_ITEMS_SHEET_NAME).select(
        pl.col("Type"),
        pl.col("No."),
        pl.col("Description"),
        pl.col("Variant"),
        pl.col("Unit Price").cast(pl.Float64).round(3),
    )
    return lf


# Overtime %
OVERTIME_150: float = 1.5
OVERTIME_200: float = 2.0
NORMAL_HOUR: float = 1.0
