"""Stuffing Lazyframes.

``coa`` and ``pallet`` are ``@lru_cache`` builders exposed under their old
names via :pep:`562` ``__getattr__``; importing this module does no I/O.
"""

from datetime import timedelta
from functools import lru_cache
from typing import Any

import polars as pl

from data.price import get_price
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import (
    LINER_PALLET_SHEET_NAME,
    PLUGIN_SHEET_NAME,
    STUFFING_SHEET_ID,
)
from type_casting import (
    CURRENT_YEAR,
    PLUGGED_STATUS,
    PalletType,
    containers_enum,
    enum_customer,
    shipper,
    shipping_line,
)


# Price
@lru_cache(maxsize=1)
def _prices() -> dict[str, float]:
    """Stuffing unit prices from one cached read of the price table."""

    def _p(service: str) -> float:
        return get_price([service]).select(pl.col("unit_price")).to_series()[0]

    return {
        "liner": _p("Plastic Liner Installation"),
        "magnum_electricity": _p("Electricity Price Magnum"),
        "monitoring": _p("Monitoring"),
        "pallet_iot": _p("Pallets(+ Wedges) Usage"),
        "pallet": _p("Pallets"),
        "plugin": _p("Plugin"),
        "s_freezer_electricity": _p("Electricity Price S Freezer"),
        "standard_electricity": _p("Electricity Price Standard"),
    }


# Yard Metrics
transfer_direct: pl.Expr = pl.col("operation_type").str.contains("Direct")
exchange_hands: pl.Expr = pl.col("operation_type").str.contains("Exchange")


on_plug_or_partially_stuffed: pl.Expr = pl.col("location").is_in(["For Completion", "On Plug"])
on_plug: pl.Expr = pl.col("location").is_in(["On Plug"])
partially_stuffed: pl.Expr = pl.col("location").is_in(["For Completion"])
plugged_only: pl.Expr = pl.col("location") == "Plugin Only"

# Durations
duration: pl.Expr = ((pl.col("date_out") - pl.col("date_plugged")).dt.total_hours() / 24).cast(
    pl.Int64
)


def load_pallet_dataset() -> pl.LazyFrame:
    """load the pallet and liner datasets"""
    return (
        load_gsheet_data(sheet_id=STUFFING_SHEET_ID, sheet_name=LINER_PALLET_SHEET_NAME)
        .filter(pl.col("date").dt.year().eq(CURRENT_YEAR))
        .select(
            pl.col("date"),
            pl.col("container_number").cast(dtype=containers_enum),
            pl.col("shipping_line").cast(dtype=pl.Enum(shipping_line + ["SAPMER"])),
            pl.col("assigned_to").str.to_uppercase(),
            pl.col("remarks").cast(dtype=PalletType.enum_dtype()),
        )
    )


# Pallet and Liner Dataframe
@lru_cache(maxsize=1)
def _pallet() -> pl.LazyFrame:
    prices = _prices()
    PALLET_IOT_PRICE = prices["pallet_iot"]
    PALLET_PRICE = prices["pallet"]
    LINER_PRICE = prices["liner"]
    return load_pallet_dataset().with_columns(
        pallet_price=pl.when(
            (pl.col("remarks").cast(pl.Utf8).str.contains(pl.lit("Pallet"), strict=True)).and_(
                pl.col("shipping_line").eq(pl.lit("IOT"))
            )
        )
        .then(PALLET_IOT_PRICE)
        .when(pl.col("remarks").cast(pl.Utf8).str.contains(pl.lit("Pallet"), strict=True))
        .then(PALLET_PRICE)
        .otherwise(0),
        liner_price=pl.when(
            (pl.col("remarks").cast(pl.Utf8).str.contains(pl.lit("Liner"), strict=True)).and_(
                pl.col("shipping_line").eq(pl.lit("CMA CGM"))
            )
        )
        .then(LINER_PRICE)
        .otherwise(0),
    )


@lru_cache(maxsize=1)
def _coa() -> pl.LazyFrame:
    prices = _prices()
    PLUGIN_PRICE = prices["plugin"]
    MONITORING_PRICE = prices["monitoring"]
    S_FREEZER_ELECTRICITY = prices["s_freezer_electricity"]
    MAGNUM_ELECTRICITY = prices["magnum_electricity"]
    STANDARD_ELECTRICITY = prices["standard_electricity"]
    return (
        load_gsheet_data(sheet_id=STUFFING_SHEET_ID, sheet_name=PLUGIN_SHEET_NAME)
        .filter(pl.col("date_out").dt.year().eq(CURRENT_YEAR).or_(pl.col("date_out").is_null()))
        .select(
            pl.col("vessel_client").str.to_uppercase().cast(dtype=enum_customer()),
            pl.col("customer").cast(dtype=pl.Enum(shipper)),
            pl.col("date_plugged"),
            pl.col("time_plugged").cast(dtype=pl.Time),
            pl.col("container_number").cast(dtype=containers_enum),
            pl.col("operation_type"),
            pl.col("shipping_line").cast(dtype=pl.Enum(shipping_line)),
            pl.col("plugged_status").cast(dtype=pl.Enum(PLUGGED_STATUS)),
            pl.col("tonnage"),
            pl.col("set_point"),
            pl.col("date_out"),
            pl.col("location"),
        )
        .with_columns(
            pl.col("date_plugged").cast(pl.Date, strict=False),
            pl.col("date_out").cast(pl.Date, strict=False),
        )
        .with_columns(
            days_on_plug=pl.when(transfer_direct | on_plug | plugged_only)
            .then(timedelta(days=0))
            .when(partially_stuffed)
            .then(duration)
            .otherwise(duration + 1),
            plugin_price=pl.when(transfer_direct | exchange_hands).then(0).otherwise(PLUGIN_PRICE),
            monitoring_price=pl.when(transfer_direct | on_plug_or_partially_stuffed | plugged_only)
            .then(0)
            .otherwise(MONITORING_PRICE),
        )
        .with_columns(
            electricity_unit_price=pl.when(plugged_only)
            .then(pl.lit(0))
            .when(pl.col("set_point").eq(-60))
            .then(S_FREEZER_ELECTRICITY)
            .when(pl.col("set_point").eq(-35))
            .then(MAGNUM_ELECTRICITY)
            .otherwise(STANDARD_ELECTRICITY)
        )
        .with_columns(total_electricity=pl.col("electricity_unit_price") * pl.col("days_on_plug"))
        .with_columns(
            total=pl.col("plugin_price") + pl.col("monitoring_price") + pl.col("total_electricity")
        )
    )


_EXPORTS = {
    "coa": _coa,
    "pallet": _pallet,
}


def __getattr__(name: str) -> Any:
    builder = _EXPORTS.get(name)
    if builder is not None:
        return builder()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [*_EXPORTS, "load_pallet_dataset"]
