"""Miscellaneous Dataframe.

Every frame is an ``@lru_cache`` builder exposed under its historical name
via :pep:`562` ``__getattr__`` -- importing this module does no I/O.
"""

from functools import lru_cache
from typing import Any

import polars as pl

from data.price import (
    get_price,
)
from data_source.all_dataframe import (
    _cross_stuffing,
    by_catch_transfer,
    cccs_container_stuffing,
    miscellaneous,
    via_skiff_transfer,
)
from type_casting.customers import bycatch, client_shore_cost
from type_casting.dates import SPECIAL_DAYS
from type_casting.validations import (
    CARGO_DISPATCH_SERVICE,
    UNLOADING_SERVICE,
    MovementType,
    OvertimePerc,
)


# Price
@lru_cache(maxsize=1)
def _prices() -> dict[str, Any]:
    """Miscellaneous prices from one cached read of the price table."""
    return {
        "truck": get_price(["Tipping Truck"]),
        "cargo_loading": get_price(["Loading to Cargo"]),
        "cccs_movement_fee": (
            get_price(["CCCS Movement in/out"])
            .select(pl.col("unit_price"))
            .to_series()[0]
        ),
        "cross_stuffing": get_price(
            [
                "Cross Stuffing",
                "Unstuffing by Hand",
                "Unstuffing to Cargo",
                "Unstuffing to CCCS",
            ]
        ),
        "by_catch": get_price(["CCCS (By-Catch)", "Transfer of By-catch"]),
        "cccs_stuffing": get_price(
            [
                "Shore Crane & Fishloader",
                "Shore Crane & Fishloader (by catch)",
                "Static Loader",
                "Container Stuffing by Hand",
                "Container Stuffing with Forklift",
            ]
        ),
        "static_loader": get_price(["Static Loader"]),
    }


# Static Loader Dataset for IOT Bin Dispatch only
@lru_cache(maxsize=1)
def _static_loader() -> pl.LazyFrame:
    STATIC_LOADER = _prices()["static_loader"]
    return (
        miscellaneous()
        .select(
            pl.col("day_name"),
            pl.col("date"),
            pl.col("customer"),
            pl.col("operation_type"),
            pl.col("static_loader").cast(pl.Float64),
            pl.col("overtime_tonnage").cast(pl.Float64),
        )
        .filter(pl.col("static_loader") > 0)
        .with_columns(
            service=pl.when(pl.col("operation_type").str.contains("Dispatch"))
            .then(pl.lit("Static Loader"))
            .otherwise(pl.lit(""))
        )
        .filter(pl.col("service") == pl.lit("Static Loader"))
        .sort(by=pl.col("date"))
        .join_asof(
            STATIC_LOADER.lazy(),
            by="service",
            left_on="date",
            right_on="date",
            strategy="backward",
        )
        # .select(pl.all().exclude(["date"]))
        .with_columns(
            total_price=pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
            .then(
                (
                    pl.col("unit_price")
                    * (pl.col("static_loader") - pl.col("overtime_tonnage"))
                    * OvertimePerc.overtime_150
                )
                + (
                    pl.col("unit_price")
                    * (pl.col("overtime_tonnage"))
                    * OvertimePerc.overtime_200
                )
            )
            .otherwise(
                (
                    pl.col("unit_price")
                    * (pl.col("static_loader") - pl.col("overtime_tonnage"))
                    * OvertimePerc.normal_hour
                )
                + (
                    pl.col("unit_price")
                    * pl.col("overtime_tonnage")
                    * OvertimePerc.overtime_150
                )
            )
        )
    )


# Dispatch to Cargo
@lru_cache(maxsize=1)
def _dispatch_to_cargo() -> pl.LazyFrame:
    _p = _prices()
    TRUCK_PRICE = _p["truck"]
    CARGO_LOADING_PRICE = _p["cargo_loading"]
    CCCS_MOVEMENT_FEE = _p["cccs_movement_fee"]
    return (
        miscellaneous()
        .filter(pl.col("operation_type").is_in(CARGO_DISPATCH_SERVICE))
        .select(
            pl.col("day_name"),
            pl.col("date"),
            pl.col("movement_type"),
            pl.col("customer"),
            pl.col("vessel"),
            pl.col("operation_type"),
            pl.col("storage_type"),
            pl.col("total_tonnage").abs(),
            pl.col("overtime_tonnage").cast(pl.Float64),
        )
        .with_columns(service=pl.lit("Tipping Truck"))
        .sort(by="date")
        .join_asof(
            TRUCK_PRICE.lazy(),
            by="service",
            left_on="date",
            right_on="date",
            strategy="backward",
        )
        .select(pl.all().exclude(["service"]))
        .with_columns(
            normal_tonnage=pl.col("total_tonnage") - pl.col("overtime_tonnage"),
            service=pl.lit("Loading to Cargo"),
            cccs_movement_fee=pl.lit(CCCS_MOVEMENT_FEE),
        )
        .sort(by="date")
        .join_asof(
            CARGO_LOADING_PRICE.lazy(),
            by="service",
            left_on="date",
            right_on="date",
            strategy="backward",
        )
        .with_columns(
            price=pl.col("unit_price")
            + pl.col("unit_price_right")
            + pl.col("cccs_movement_fee")
        )
        .with_columns(
            total_price=pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
            .then(
                (pl.col("normal_tonnage") * OvertimePerc.overtime_150 * pl.col("price"))
                + (
                    pl.col("overtime_tonnage")
                    * OvertimePerc.overtime_200
                    * pl.col("price")
                )
            )
            .otherwise(
                (pl.col("normal_tonnage") * OvertimePerc.normal_hour * pl.col("price"))
                + (
                    pl.col("overtime_tonnage")
                    * OvertimePerc.overtime_150
                    * pl.col("price")
                )
            ),
        )
        .select(pl.all().exclude(["service", "normal_tonnage", "price"]))
        # .with_columns(
        #     price=pl.when(
        #         pl.col("customer").eq("DARDANEL").and_(pl.col("date").lt(date(2025, 9, 1)))
        #     )
        #     .then(pl.col("unit_price") - 1.0)  # Need a better way to represent this 1.0.
        #     .otherwise(pl.col("unit_price")),
        #     cccs_movement_fee=pl.when(
        #         pl.col("customer").eq("DARDANEL").and_(pl.col("date").lt(date(2025, 9, 1)))
        #     )
        #     .then(pl.lit(0))
        #     .otherwise(pl.col("cccs_movement_fee")),
        #     Price_right=pl.when(
        #         pl.col("customer").eq("DARDANEL").and_(pl.col("date").lt(date(2025, 9, 1)))
        #     )
        #     .then(
        #         pl.col("unit_price_right") - 3.0
        #     )  # Need a better way to represent this 3.0.
        #     .otherwise(pl.col("unit_price_right")),
        # )
        .select(
            pl.col("day_name").alias("day_name"),
            pl.col("date"),
            pl.col("movement_type").cast(dtype=MovementType.enum_dtype()),
            pl.col("customer"),
            pl.col("vessel"),
            pl.col("operation_type"),
            pl.col("storage_type"),
            pl.col("total_tonnage"),
            pl.col("overtime_tonnage"),
            pl.col("unit_price").alias("truck_price"),
            pl.col("cccs_movement_fee"),
            pl.col("unit_price_right").alias("stevedores_on_cargo_fee"),
            pl.col("total_price"),
        )
    )


@lru_cache(maxsize=1)
def _from_cccs_to_vessel() -> pl.LazyFrame:
    _p = _prices()
    TRUCK_PRICE = _p["truck"]
    CCCS_MOVEMENT_FEE = _p["cccs_movement_fee"]
    return (
        miscellaneous()
        .filter(
            pl.col("operation_type").is_in(pl.lit("From CCCS to Vessel")),
        )
        .select(
            pl.col("day_name"),
            pl.col("date"),
            pl.col("customer"),
            pl.col("vessel"),
            pl.col("total_tonnage").abs(),
            pl.col("overtime_tonnage").cast(pl.Float64),
        )
        .sort(by="date", descending=False)
        .with_columns(service=pl.lit("Tipping Truck", dtype=pl.Utf8))
        .join_asof(
            TRUCK_PRICE.lazy(),
            by="service",
            left_on="date",
            right_on="date",
            strategy="backward",
        )
        .with_columns(
            CCCS_incoming_fee=CCCS_MOVEMENT_FEE,
            operation_type=pl.lit("From CCCS to Vessel"),
        )
        .with_columns(unit_price=pl.col("unit_price") + pl.col("CCCS_incoming_fee"))
        .drop(["service", "CCCS_incoming_fee"])
        .with_columns(
            total_price=pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
            .then(
                (
                    (pl.col("total_tonnage") - pl.col("overtime_tonnage"))
                    * pl.col("unit_price")
                    * OvertimePerc.overtime_150
                )
                + (
                    pl.col("overtime_tonnage")
                    * pl.col("unit_price")
                    * OvertimePerc.overtime_200
                )
            )
            .otherwise(
                (
                    (pl.col("total_tonnage") - pl.col("overtime_tonnage"))
                    * pl.col("unit_price")
                )
                + (
                    pl.col("overtime_tonnage")
                    * pl.col("unit_price")
                    * OvertimePerc.overtime_150
                )
            )
        )
    )


# ).select(pl.all().exclude(["Service", "Date"]))


# Transfer using IPHS truck for IOT and DARDANEL
@lru_cache(maxsize=1)
def _truck_to_cccs() -> pl.LazyFrame:
    _p = _prices()
    TRUCK_PRICE = _p["truck"]
    CCCS_MOVEMENT_FEE = _p["cccs_movement_fee"]
    return (
        miscellaneous()
        .filter(
            pl.col("customer").is_in(client_shore_cost),
            pl.col("operation_type").is_in(UNLOADING_SERVICE),
        )
        .select(
            pl.col("day_name"),
            pl.col("date"),
            pl.col("customer"),
            pl.col("vessel"),
            pl.col("total_tonnage"),
            pl.col("overtime_tonnage").cast(pl.Float64),
        )
        .group_by(["day_name", "date", "customer", "vessel"])
        .agg(pl.col("total_tonnage").sum(), pl.col("overtime_tonnage").sum())
        .sort(by="date", descending=False)
        .with_columns(service=pl.lit("Tipping Truck", dtype=pl.Utf8))
        .join_asof(
            TRUCK_PRICE.lazy(),
            by="service",
            left_on="date",
            right_on="date",
            strategy="backward",
        )
        .with_columns(
            CCCS_incoming_fee=CCCS_MOVEMENT_FEE,
            operation_type=pl.lit("IPHS Truck to CCCS"),
        )
        .with_columns(
            total_price=pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
            .then(
                (
                    (
                        (pl.col("total_tonnage") - pl.col("overtime_tonnage"))
                        * pl.col("unit_price")
                        * OvertimePerc.overtime_150
                    )
                    + (
                        pl.col("overtime_tonnage")
                        * pl.col("unit_price")
                        * OvertimePerc.overtime_200
                    )
                )
                + (
                    (
                        (pl.col("total_tonnage") - pl.col("overtime_tonnage"))
                        * pl.col("CCCS_incoming_fee")
                        * OvertimePerc.overtime_150
                    )
                    + (
                        pl.col("overtime_tonnage")
                        * pl.col("CCCS_incoming_fee")
                        * OvertimePerc.overtime_200
                    )
                )
            )
            .otherwise(
                (
                    (
                        (pl.col("total_tonnage") - pl.col("overtime_tonnage"))
                        * pl.col("unit_price")
                    )
                    + (
                        pl.col("overtime_tonnage")
                        * pl.col("unit_price")
                        * OvertimePerc.overtime_150
                    )
                )
                + (
                    (
                        (pl.col("total_tonnage") - pl.col("overtime_tonnage"))
                        * pl.col("CCCS_incoming_fee")
                    )
                    + (
                        pl.col("overtime_tonnage")
                        * pl.col("CCCS_incoming_fee")
                        * OvertimePerc.overtime_150
                    )
                )
            )
        )
    ).select(pl.all().exclude(["service"]))


# Cross stuffing and Unstuffing dataset
@lru_cache(maxsize=1)
def _cross_stuffing_frame() -> pl.LazyFrame:
    CROSS_STUFFING_PRICE = _prices()["cross_stuffing"]
    return (
        _cross_stuffing()
        .join_asof(
            CROSS_STUFFING_PRICE.lazy(),
            by="service",
            left_on="date",
            right_on="date",
            strategy="backward",
        )
        .with_columns(
            unit_price=pl.when(
                pl.col("invoiced")
                .is_in(["AMIRANTE", "OCEAN BASKET", "ISLAND CATCH"])
                .and_(pl.col("service").eq(pl.lit("Unstuffing to CCCS")))
            )
            .then(pl.col("unit_price") - 3.5)
            .otherwise(
                pl.col("unit_price")  # Adjusting price for specific customers
            )  # Adjusting price for specific customers
        )
        .with_columns(normal_hours=pl.col("total_tonnage") - pl.col("overtime_tonnage"))
        .with_columns(
            total_price=pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
            .then(
                (
                    pl.col("normal_hours")
                    * OvertimePerc.overtime_150
                    * pl.col("unit_price")
                )
                + (
                    pl.col("overtime_tonnage")
                    * OvertimePerc.overtime_200
                    * pl.col("unit_price")
                )
            )
            .otherwise(
                (
                    pl.col("normal_hours")
                    * OvertimePerc.normal_hour
                    * pl.col("unit_price")
                )
                + (
                    pl.col("overtime_tonnage")
                    * OvertimePerc.overtime_150
                    * pl.col("unit_price")
                )
            )
            .round(3)
        )
        .select(pl.all().exclude(["normal_hours"]))
    )


# BY CATCH RECORDS
by_catch_companies = bycatch  # To make this workable


# Filter only bycatch services
@lru_cache(maxsize=1)
def _by_catch_base() -> pl.LazyFrame:
    return (
        miscellaneous()
        .filter(
            pl.col("operation_type").is_in(UNLOADING_SERVICE),
            pl.col("customer").is_in(by_catch_companies),
            # pl.col("date").dt.year().eq(CURRENT_YEAR),
        )
        .select(
            pl.col("day_name"),
            pl.col("date"),
            pl.col("movement_type"),
            pl.col("customer"),
            pl.col("vessel"),
            pl.col("operation_type")
            .str.replace("Sorting from Unloading", "CCCS (By-Catch)")
            .str.replace("Unsorted from Unloading", "CCCS (By-Catch)"),
            pl.col("storage_type"),
            pl.col("total_tonnage").cast(pl.Float64).round(3),
            pl.col("overtime_tonnage").cast(pl.Float64).round(3),
        )
        .group_by(
            [
                "day_name",
                "date",
                "movement_type",
                "customer",
                "vessel",
                "operation_type",
                "storage_type",
            ]
        )
        .agg(pl.col("total_tonnage").sum(), pl.col("overtime_tonnage").sum())
        .sort(by="date")
    )


# Records which has both CCCS (by-catch) and transfer of by-catch
@lru_cache(maxsize=1)
def _by_catch_with_transfer() -> pl.LazyFrame:
    return (
        _by_catch_base()
        .join(
            by_catch_transfer(),
            on=[
                "date",
                "customer",
                "vessel",
                "movement_type",
            ],
            how="left",
        )
        .filter(pl.col("total_tonnage_right").is_not_null())
        .with_columns(
            total_tonnage=(pl.col("total_tonnage") - pl.col("total_tonnage_right"))
            .cast(pl.Float64)
            .round(3),
            overtime_tonnage=(
                pl.col("overtime_tonnage") - pl.col("overtime_tonnage_right")
            ).round(3),
        )
        .filter(pl.col("total_tonnage").ne(0))
        .select(
            [
                "day_name",
                "date",
                "movement_type",
                "customer",
                "vessel",
                "operation_type",
                "storage_type",
                "total_tonnage",
                "overtime_tonnage",
            ]
        )
    )


# Services which has only by-catch
@lru_cache(maxsize=1)
def _by_catch_only() -> pl.LazyFrame:
    return (
        _by_catch_base()
        .join(
            _by_catch_with_transfer(),
            on=[
                "date",
                "customer",
                "vessel",
                "movement_type",
            ],
            how="left",
        )
        .filter(pl.col("operation_type_right").is_null())
        .select(
            [
                "day_name",
                "date",
                "movement_type",
                "customer",
                "vessel",
                "operation_type",
                "storage_type",
                "total_tonnage",
                "overtime_tonnage",
            ]
        )
        .join(
            by_catch_transfer(),
            on=[
                "date",
                "customer",
                "vessel",
                "movement_type",
            ],
            how="left",
        )
        .filter(pl.col("operation_type_right").is_null())
        .select(
            [
                "day_name",
                "date",
                "movement_type",
                "customer",
                "vessel",
                "operation_type",
                "storage_type",
                "total_tonnage",
                "overtime_tonnage",
            ]
        )
    )


# Final By Catch Record
@lru_cache(maxsize=1)
def _by_catch() -> pl.LazyFrame:
    BY_CATCH_PRICE = _prices()["by_catch"]
    return (
        pl.concat(
            [_by_catch_only(), by_catch_transfer(), _by_catch_with_transfer()],
            how="vertical",
        )
        .sort(by="date")
        .with_columns(pl.col("operation_type").alias("service"))
        .sort(by="date")
        .join_asof(
            BY_CATCH_PRICE.lazy(),
            by="service",
            left_on="date",
            right_on="date",
            strategy="backward",
        )
        .with_columns(normal_hours=pl.col("total_tonnage") - pl.col("overtime_tonnage"))
        .with_columns(
            total_price=pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
            .then(
                (
                    pl.col("normal_hours")
                    * OvertimePerc.overtime_150
                    * pl.col("unit_price")
                )
                + (
                    pl.col("overtime_tonnage")
                    * OvertimePerc.overtime_200
                    * pl.col("unit_price")
                )
            )
            .otherwise(
                (
                    pl.col("normal_hours")
                    * OvertimePerc.normal_hour
                    * pl.col("unit_price")
                )
                + (
                    pl.col("overtime_tonnage")
                    * OvertimePerc.overtime_150
                    * pl.col("unit_price")
                )
            )
        )
        .select(pl.all().exclude(["movement_type", "service", "normal_hours"]))
    )


# CCCS Container Stuffing dataset
@lru_cache(maxsize=1)
def _cccs_stuffing() -> pl.LazyFrame:
    CCCS_STUFFING_PRICE = _prices()["cccs_stuffing"]
    return (
        cccs_container_stuffing()
        .join_asof(
            CCCS_STUFFING_PRICE.lazy(),
            by="service",
            left_on="date",
            right_on="date",
            strategy="backward",
        )
        .with_columns(normal_hours=pl.col("total_tonnage") - pl.col("overtime_tonnage"))
        .with_columns(
            total_price=pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
            .then(
                (
                    pl.col("normal_hours")
                    * OvertimePerc.overtime_150
                    * pl.col("unit_price")
                )
                + (
                    pl.col("overtime_tonnage")
                    * OvertimePerc.overtime_200
                    * pl.col("unit_price")
                )
            )
            .otherwise(
                (
                    pl.col("normal_hours")
                    * OvertimePerc.normal_hour
                    * pl.col("unit_price")
                )
                + (
                    pl.col("overtime_tonnage")
                    * OvertimePerc.overtime_150
                    * pl.col("unit_price")
                )
            )
        )
        .select(pl.all().exclude(["normal_hours"]))
    )


# Transfer of Fish Via Skiff


@lru_cache(maxsize=1)
def _truck_to_cccs_via_skiff() -> pl.LazyFrame:
    TRUCK_PRICE = _prices()["truck"]
    return (
        via_skiff_transfer()
        .sort(by="date")
        .with_columns(pl.col("operation_type").alias("service"))
        .sort(by="date")
        .join_asof(
            TRUCK_PRICE.lazy(),
            by="service",
            left_on="date",
            right_on="date",
            strategy="backward",
        )
        .with_columns(normal_hours=pl.col("total_tonnage") - pl.col("overtime_tonnage"))
        .with_columns(
            total_price=pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
            .then(
                (
                    pl.col("normal_hours")
                    * OvertimePerc.overtime_150
                    * pl.col("unit_price")
                )
                + (
                    pl.col("overtime_tonnage")
                    * OvertimePerc.overtime_200
                    * pl.col("unit_price")
                )
            )
            .otherwise(
                (
                    pl.col("normal_hours")
                    * OvertimePerc.normal_hour
                    * pl.col("unit_price")
                )
                + (
                    pl.col("overtime_tonnage")
                    * OvertimePerc.overtime_150
                    * pl.col("unit_price")
                )
            )
        )
        .select(pl.all().exclude(["movement_type", "service", "normal_hours"]))
    )


_EXPORTS = {
    "static_loader": _static_loader,
    "dispatch_to_cargo": _dispatch_to_cargo,
    "from_cccs_to_vessel": _from_cccs_to_vessel,
    "truck_to_cccs": _truck_to_cccs,
    "cross_stuffing": _cross_stuffing_frame,
    "by_catch": _by_catch,
    "cccs_stuffing": _cccs_stuffing,
    "truck_to_cccs_via_skiff": _truck_to_cccs_via_skiff,
}


def __getattr__(name: str) -> Any:
    builder = _EXPORTS.get(name)
    if builder is not None:
        return builder()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = list(_EXPORTS)
