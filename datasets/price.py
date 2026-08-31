"""Pricing ModulePrice lookups for the net-list pipelines.

All frames are explicitly sorted for ``join_asof``: Polars requires the
right frame ordered on the ``on`` key (within ``by`` groups when used).
The old module relied on ``get_price`` happening to return sorted data.
"""

from __future__ import annotations

from functools import cache, lru_cache

import polars as pl

from data_source.make_dataset import load_sheet as scan_google_sheet

# from data_source.sheet_ids import MASTER_ID, PRICE_SHEET_NAME
from utils import PolarsEnum

# Master Validation / Pricing
MASTER_ID: str = "1ai-zQMtbPUx0LeQeLmXcpPgKvL5cyvwDfSJqRzxfUQg"
PRICE_SHEET_NAME: str = "Price"
CLIENT_SHEET_NAME: str = "Client"


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

    # Blank EndingDate = currently-active price. Cast to Utf8 first: the
    # 0.3.0 parser may type this column as Date (blank -> null) where the
    # old one left it Utf8 (blank -> "").
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


# Overtime %
OVERTIME_150: float = 1.5
OVERTIME_200: float = 2.0
NORMAL_HOUR: float = 1.0


def unit_price(service: str) -> float:
    """Unit price for exactly one service.

    Uses ``.item()``, which raises if the lookup returns zero or
    multiple rows — unlike ``to_series()[0]``, which silently grabbed
    whichever row came first.
    """

    return get_price([service]).select("unit_price").item()


def unit_price_by_type(service_type, service: str) -> float:
    """Unit price for exactly one service within a service type.

    Filter *before* select — the old washing module selected
    ``unit_price`` first and then filtered on the dropped ``service``
    column, a ColumnNotFoundError at collect time.
    """
    from data import get_price_by_type  # local import: keep module side-effect free

    return (
        get_price_by_type(service_type=service_type)
        .filter(pl.col("service").eq(service))
        .select("unit_price")
        .collect()
        .item()
    )


UNLOADING_SERVICES: list[str] = [
    "Transhipment - Brine",
    "Transhipment - Dry",
    "Unload to CCCS - Brine",
    "Unload to CCCS - Dry",
    "Unload to Quay - Brine",
    "Unload to Quay - Dry",
    "Container Stuffing - Brine",
    "Container Stuffing - Dry",
    "Full OSS - Brine",
    "Full OSS - Dry",
    "Basic OSS - Brine",
    "Basic OSS - Dry",
]

OSS_STUFFING_SERVICES: list[str] = [
    "Container Stuffing - Brine",
    "Container Stuffing - Dry",
    "Stuffing",
]


@cache
def _sorted_price(services: tuple[str, ...]) -> pl.LazyFrame:
    return get_price(list(services)).sort("service", "date").lazy()


def unloading_price() -> pl.LazyFrame:
    return _sorted_price(tuple(UNLOADING_SERVICES))


def oss_stuffing_price() -> pl.LazyFrame:
    return _sorted_price(tuple(OSS_STUFFING_SERVICES))


def stuffing_price() -> pl.LazyFrame:
    return _sorted_price(("Stuffing",))


def iot_cargo_price() -> pl.LazyFrame:
    return _sorted_price(("Loading to Cargo - Brine", "Loading to Cargo - Dry"))
