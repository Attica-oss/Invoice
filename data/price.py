"""Pricing Module"""

from __future__ import annotations
from functools import lru_cache


import polars as pl

from read_google_sheet import read_google_sheet

from data_source.sheet_ids import MASTER_ID, price_sheet

VALIDATION_ID = MASTER_ID
PRICE_SHEET_NAME = price_sheet

DATE_FMT = "%d/%m/%Y"


@lru_cache(maxsize=1)
def price_table() -> pl.DataFrame:
    """
    Load + clean the price sheet once (small table → keep in memory).
    """
    lf = read_google_sheet(sheet_id=MASTER_ID, sheet_name=PRICE_SHEET_NAME).unwrap()

    return (
        lf.with_columns(
            pl.col("StartingDate").alias("start"),
            pl.col("EndingDate").alias("end"),
            pl.col("Price").cast(pl.Float64),
        )
        .select(["Service", "Price", "start", "end"])
        .collect()
    )


def get_price(services: list[str] | None = None) -> pl.DataFrame:
    """Get the price table, optionally filtered by a list of services."""
    df = price_table()
    if services is None:
        return df
    return df.filter(pl.col("Service").is_in(services))


# from type_casting.dates import SPECIAL_DAYS

# Overtime %
OVERTIME_150: float = 1.5
OVERTIME_200: float = 2.0
NORMAL_HOUR: float = 1.0


list_of_services = [
    "CCCS Movement in/out",  # BIN_TIPPING_PRICE, CCCS_MOVEMENT_FEE SCOW_TRANSFER
    "Loading (Quay to Ship)",  # SALT PRICE
    "Loading @ Zone 14",  # SALT PRICE
    "Tipping Truck",  # TRUCK PRICE
    "Loading to Cargo",  # CARGO_LOADING_PRICE
    "Transfer of by-catch",  # CCCS_TRUCK, BY_CATCH_PRICE
    "Cross Stuffing",  # CROSS_STUFFING_PRICE
    "Unstuffing by Hand",  # CROSS_STUFFING_PRICE
    "Unstuffing to Cargo",  # CROSS_STUFFING_PRICE
    "Unstuffing to CCCS",  # CROSS_STUFFING_PRICE
    "CCCS (By-Catch)",  # BY_CATCH_PRICE
    "Shore Crane & Fishloader",  # CCCS_STUFFING_PRICE
    "Shore Crane & Fishloader (by catch)",  # CCCS_STUFFING_PRICE
    "Static Loader",  # CCCS_STUFFING_PRICE STATIC_LOADER
    "Container Stuffing by Hand",  # CCCS_STUFFING_PRICE
    "Shifting",  # SHIFTING SHIFTING_PRICE
    "Plugin",  # PLUGIN PLUGIN_PRICE
    "PTI S Freezer",  # S_FREEZER_PTI_ELECTRICITY
    "PTI Magnum",  # MAGNUM_PTI_ELECTRICITY
    "PTI Standard",  # STANDARD_PTI_ELECTRICITY
    "Electricity Price Standard",  # IOT_ELECTRICITY
    "Container Cleaning",  # WASHING
    "Stuffing",  # STUFFING_PRICE
    "Monitoring",  # MONITORING_PRICE
    "Electricity Price S Freezer",  # S_FREEZER_ELECTRICITY
    "Electricity Price Magnum",  # MAGNUM_ELECTRICITY
    "Electricity Price Standard",  # STANDARD_ELECTRICITY
    "Pallets",  # PALLET_PRICE
    "Plastic Liner Installation",  # LINER_PRICE
    "Pallets(+ Wedges) Usage",  # PALLET_IOT_PRICE
    "Haulage FEU",  # TRANSFER_PRICE
    "Haulage TEU",  # TRANSFER_PRICE
    "Container Stuffing - Brine",  # OSS_PRICE
    "Container Stuffing - Dry",  # OSS_PRICE
]

ALL_PRICE = get_price(list_of_services)

DARDANEL_DISCOUNT: float = 4.0  # Need a better way to represent this

FREE: pl.Expr = pl.lit(0)
