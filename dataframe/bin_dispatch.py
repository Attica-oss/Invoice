"""Bin dispatch (Scow Transfer) to and from IOT.

``full_scows`` / ``empty_scows`` are ``@lru_cache`` builders exposed under
their old names via :pep:`562` ``__getattr__``; importing this module does
no I/O.
"""

from functools import lru_cache
from typing import Any

import polars as pl

import dataframe.transport as _transport
from data.price import (
    NORMAL_HOUR,
    OVERTIME_150,
    OVERTIME_200,
    ServiceType,
    get_price_by_type,
)
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import (
    ALL_CCCS_DATA_SHEET_NAME,
    MISC_SHEET_ID,
)
from type_casting.dates import (
    CURRENT_YEAR,
    SPECIAL_DAYS,
    UPPER_BOUND,
    UPPER_BOUND_SPECIAL_DAY,
    public_holiday,
)
from type_casting.validations import (
    BIN_DISPATCH_SERVICE,
    MovementType,
    Overtime,
    Status,
)

# Prepare the list of Public Holiday dates in the Current Year
ph_list: pl.Series = public_holiday()


# Price
@lru_cache(maxsize=1)
def _scow_transfer_price() -> pl.LazyFrame:
    return get_price_by_type(service_type=ServiceType.cold_store).filter(
        pl.col("service").eq(pl.lit("CCCS Movement in/out"))
    )


# Full Scows
@lru_cache(maxsize=1)
def _bin_dispatch_base() -> pl.LazyFrame:
    return (
        load_gsheet_data(MISC_SHEET_ID, ALL_CCCS_DATA_SHEET_NAME)
        .filter(
            pl.col("service").is_in(BIN_DISPATCH_SERVICE),
            pl.col("date").dt.year().eq(CURRENT_YEAR),
        )
        .select(
            pl.col("date").days.add_day_name(),
            pl.col("date"),
            pl.col("movement_type").cast(MovementType.enum_dtype()),
            pl.col("customer"),
            pl.col("service").alias("operation_type"),
            pl.col("total_tonnage").abs().cast(pl.Float64).round(3),
            pl.col("overtime_tonnage").str.replace("", "0").cast(pl.Float64).round(3),
        )
    )


@lru_cache(maxsize=1)
def _bin_dispatch() -> pl.LazyFrame:
    return (
        _bin_dispatch_base()
        .group_by(["date", "customer", "movement_type"])
        .agg(pl.col("total_tonnage").sum(), pl.col("overtime_tonnage").sum())
        .with_columns(
            normal_tonnage=(pl.col("total_tonnage") - pl.col("overtime_tonnage"))
            .round(3)
            .cast(pl.Float64)
        )
    )


@lru_cache(maxsize=1)
def _bin_dispatch_fallback() -> pl.LazyFrame:
    return (
        _bin_dispatch_base()
        .group_by(["date", "movement_type"])
        .agg(pl.col("total_tonnage").sum(), pl.col("overtime_tonnage").sum())
        .with_columns(
            normal_tonnage=(pl.col("total_tonnage") - pl.col("overtime_tonnage"))
            .round(3)
            .cast(pl.Float64)
        )
    )


@lru_cache(maxsize=1)
def _bin_dispatch_customer_fallback() -> pl.LazyFrame:
    return (
        _bin_dispatch_base()
        .group_by(["date", "customer"])
        .agg(pl.col("total_tonnage").sum(), pl.col("overtime_tonnage").sum())
        .with_columns(
            normal_tonnage=(pl.col("total_tonnage") - pl.col("overtime_tonnage"))
            .round(3)
            .cast(pl.Float64)
        )
    )


@lru_cache(maxsize=1)
def _full_scows() -> pl.LazyFrame:
    SCOW_TRANSFER = _scow_transfer_price()
    bin_dispatch = _bin_dispatch()
    bin_dispatch_fallback = _bin_dispatch_fallback()
    bin_dispatch_customer_fallback = _bin_dispatch_customer_fallback()
    return (
        _transport.scow_transfer.filter(pl.col("status") == Status.full)
        .with_columns(
            movement_type=pl.when(pl.col("movement_type") == MovementType.delivery)
            .then(pl.lit(MovementType.out))
            .otherwise(pl.lit(MovementType.in_))
            .cast(dtype=MovementType.enum_dtype()),
            storage_type=pl.lit("Dry", dtype=pl.Utf8),
            day_name=pl.when(pl.col("date").is_in(ph_list))
            .then(pl.lit("PH"))
            .otherwise(pl.col("date").dt.to_string(format="%a")),
        )
        .with_columns(
            overtime=pl.when(
                (pl.col("day_name").is_in(SPECIAL_DAYS))
                & (pl.col("time_out") > UPPER_BOUND_SPECIAL_DAY)
            )
            .then(pl.lit(Overtime.overtime_200_text))
            .when(
                (pl.col("day_name").is_in(SPECIAL_DAYS))
                | (
                    (~pl.col("day_name").is_in(SPECIAL_DAYS))
                    & (pl.col("time_out") > UPPER_BOUND)
                )
            )
            .then(pl.lit(Overtime.overtime_150_text))
            .otherwise(pl.lit(Overtime.normal_hour_text))
        )
        .group_by(
            [
                "day_name",
                "date",
                "customer",
                "movement_type",
                "overtime",
                "storage_type",
            ]
        )
        .agg(
            pl.col("time_out").min().alias("start_time"),
            pl.col("time_in").max().alias("end_time"),
            pl.col("num_of_scows").sum(),
        )
        .join(other=bin_dispatch, on=["date", "customer", "movement_type"], how="left")
        .join(
            other=bin_dispatch_fallback,
            on=["date", "movement_type"],
            how="left",
            suffix="_fb",
        )
        .join(
            other=bin_dispatch_customer_fallback,
            on=["date", "customer"],
            how="left",
            suffix="_cfb",
        )
        .with_columns(
            tonnage=pl.when(
                (pl.col("overtime") == Overtime.normal_hour_text)
                | (
                    (pl.col("day_name").is_in(SPECIAL_DAYS))
                    & (pl.col("overtime") == Overtime.overtime_150_text)
                )
            )
            .then(
                pl.coalesce(
                    [
                        pl.col("normal_tonnage"),
                        pl.col("normal_tonnage_fb"),
                        pl.col("normal_tonnage_cfb"),
                    ]
                )
            )
            .otherwise(
                pl.coalesce(
                    [
                        pl.col("overtime_tonnage"),
                        pl.col("overtime_tonnage_fb"),
                        pl.col("overtime_tonnage_cfb"),
                    ]
                )
            )
            .cast(pl.Float64)
        )
        .select(
            pl.all().exclude(
                [
                    "total_tonnage",
                    "overtime_tonnage",
                    "normal_tonnage",
                    "total_tonnage_fb",
                    "overtime_tonnage_fb",
                    "normal_tonnage_fb",
                    "total_tonnage_cfb",
                    "overtime_tonnage_cfb",
                    "normal_tonnage_cfb",
                ]
            )
        )
        .sort(by=pl.col("date"))
        .join_asof(
            SCOW_TRANSFER.lazy(),
            by=None,
            left_on="date",
            right_on="date",
            strategy="backward",
        )
        .with_columns(
            total_price=pl.when(
                pl.col("customer").is_in(["ISLAND CATCH", "OCEAN BASKET", "AMIRANTE"])
            )
            .then(pl.lit(0.0))
            .when(pl.col("overtime") == Overtime.normal_hour_text)
            .then(pl.col("tonnage") * NORMAL_HOUR * pl.col("unit_price"))
            .when(pl.col("overtime") == Overtime.overtime_150_text)
            .then(pl.col("tonnage") * OVERTIME_150 * pl.col("unit_price"))
            .otherwise(pl.col("tonnage") * OVERTIME_200 * pl.col("unit_price"))
            .round(3)
            .cast(pl.Float64),
            movement_type=pl.when(pl.col("movement_type") == MovementType.out)
            .then(pl.lit("IPHS Delivery of Full Scows to IOT"))
            .when(pl.col("movement_type") == MovementType.in_)
            .then(pl.lit("IPHS Collection of Full Scows from IOT"))
            .otherwise(pl.lit("Err")),
        )
        .select(
            [
                "day_name",
                "date",
                "customer",
                "movement_type",
                "overtime",
                "start_time",
                "end_time",
                "num_of_scows",
                "storage_type",
                "tonnage",
                "unit_price",
                "total_price",
            ]
        )
    )


# Empty Scows
@lru_cache(maxsize=1)
def _empty_scows() -> pl.LazyFrame:
    SCOW_TRANSFER = _scow_transfer_price()
    return (
        _transport.scow_transfer.filter(pl.col("status") == Status.empty)
        .with_columns(
            movement_type=pl.when(pl.col("movement_type") == MovementType.delivery)
            .then(pl.lit(MovementType.out))
            .otherwise(pl.lit(MovementType.in_))
            .cast(dtype=MovementType.enum_dtype()),
            day_name=pl.when(pl.col("date").is_in(ph_list))
            .then(pl.lit("PH"))
            .otherwise(pl.col("date").dt.to_string(format="%a")),
        )
        .with_columns(
            overtime=pl.when(
                (pl.col("day_name").is_in(SPECIAL_DAYS))
                & (pl.col("time_out") > UPPER_BOUND_SPECIAL_DAY)
            )
            .then(pl.lit(Overtime.overtime_200_text))
            .when(
                (pl.col("day_name").is_in(SPECIAL_DAYS))
                | (
                    (~pl.col("day_name").is_in(SPECIAL_DAYS))
                    & (pl.col("time_out") > UPPER_BOUND)
                )
            )
            .then(pl.lit(Overtime.overtime_150_text))
            .otherwise(pl.lit(Overtime.normal_hour_text))
        )
        .group_by(["day_name", "date", "customer", "movement_type", "overtime"])
        .agg(
            pl.col("time_out").min().alias("start_time"),
            pl.col("time_in").max().alias("end_time"),
            pl.col("num_of_scows").sum(),
        )
        .sort(by="date")
        .join_asof(
            SCOW_TRANSFER.lazy(),
            by=None,
            left_on="date",
            right_on="date",
            strategy="backward",
        )
        .with_columns(
            total_price=pl.when(pl.col("overtime") == Overtime.normal_hour_text)
            .then(pl.col("num_of_scows") * NORMAL_HOUR * pl.col("unit_price"))
            .when(pl.col("overtime") == Overtime.overtime_150_text)
            .then(pl.col("num_of_scows") * OVERTIME_150 * pl.col("unit_price"))
            .otherwise(pl.col("num_of_scows") * OVERTIME_200 * pl.col("unit_price")),
            movement_type=pl.when(pl.col("movement_type") == MovementType.out)
            .then(pl.lit("IPHS Delivery of Empty Scows to IOT"))
            .when(pl.col("movement_type") == MovementType.in_)
            .then(pl.lit("IPHS Collection of Empty Scows from IOT"))
            .otherwise(pl.lit("Err")),
        )
        .select(pl.all().exclude(["Service"]))
    )


_EXPORTS = {
    "full_scows": _full_scows,
    "empty_scows": _empty_scows,
}


def __getattr__(name: str) -> Any:
    builder = _EXPORTS.get(name)
    if builder is not None:
        return builder()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = list(_EXPORTS)
