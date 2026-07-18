"""Pricing Module"""

from __future__ import annotations

from functools import lru_cache

import polars as pl
from scan_google_sheet import scan_google_sheet

from data_source.sheet_ids import MASTER_ID, PRICE_SHEET_NAME
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

    return lf.filter(pl.col("EndingDate").eq("")).select(
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


# Overtime %
OVERTIME_150: float = 1.5
OVERTIME_200: float = 2.0
NORMAL_HOUR: float = 1.0
