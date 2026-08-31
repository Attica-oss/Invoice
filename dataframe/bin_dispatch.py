"""Bin dispatch (Scow Transfer) to and from IOT.

``full_scows`` and ``empty_scows`` are built by cached factory functions and
exposed under their public names via :pep:`562` ``__getattr__``, so importing
this module performs no I/O.
"""

from __future__ import annotations

from collections.abc import Callable
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
from data_source.sheet_ids import ALL_CCCS_DATA_SHEET_NAME, MISC_SHEET_ID
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

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

SCOW_TRANSFER_SERVICE = "CCCS Movement in/out"

#: Customers that are not charged for full scow transfers.
FREE_OF_CHARGE_CUSTOMERS: tuple[str, ...] = (
    "ISLAND CATCH",
    "OCEAN BASKET",
    "AMIRANTE",
)

#: Join suffixes for the primary bin-dispatch table and its two fallbacks.
_FALLBACK_SUFFIXES: tuple[str, ...] = ("", "_fb", "_cfb")

#: Tonnage helper columns that are only needed while resolving the fallbacks.
_TONNAGE_HELPER_COLUMNS: list[str] = [
    f"{column}{suffix}"
    for suffix in _FALLBACK_SUFFIXES
    for column in ("total_tonnage", "overtime_tonnage", "normal_tonnage")
]


# --------------------------------------------------------------------------- #
# Shared expressions
# --------------------------------------------------------------------------- #


@lru_cache(maxsize=1)
def _public_holidays() -> pl.Series:
    """Public holiday dates for the current year."""
    return public_holiday()


def _day_name_expr() -> pl.Expr:
    """``PH`` for public holidays, otherwise the abbreviated weekday name."""
    return (
        pl.when(pl.col("date").is_in(_public_holidays()))
        .then(pl.lit("PH"))
        .otherwise(pl.col("date").dt.to_string(format="%a"))
        .alias("day_name")
    )


def _overtime_expr() -> pl.Expr:
    """Overtime band derived from the day name and the time out."""
    is_special_day = pl.col("day_name").is_in(SPECIAL_DAYS)
    return (
        pl.when(is_special_day & (pl.col("time_out") > UPPER_BOUND_SPECIAL_DAY))
        .then(pl.lit(Overtime.overtime_200_text))
        .when(is_special_day | (pl.col("time_out") > UPPER_BOUND))
        .then(pl.lit(Overtime.overtime_150_text))
        .otherwise(pl.lit(Overtime.normal_hour_text))
        .alias("overtime")
    )


def _normalised_movement_type_expr() -> pl.Expr:
    """Collapse the raw movement type onto ``out`` / ``in_``."""
    return (
        pl.when(pl.col("movement_type") == MovementType.delivery)
        .then(pl.lit(MovementType.out))
        .otherwise(pl.lit(MovementType.in_))
        .cast(MovementType.enum_dtype())
        .alias("movement_type")
    )


def _movement_label_expr(status: str) -> pl.Expr:
    """Human readable movement label, e.g. ``IPHS Delivery of Full Scows to IOT``."""
    return (
        pl.when(pl.col("movement_type") == MovementType.out)
        .then(pl.lit(f"IPHS Delivery of {status} Scows to IOT"))
        .when(pl.col("movement_type") == MovementType.in_)
        .then(pl.lit(f"IPHS Collection of {status} Scows from IOT"))
        .otherwise(pl.lit("Err"))
        .alias("movement_type")
    )


def _rated_price_expr(quantity: pl.Expr) -> pl.Expr:
    """Apply the overtime multiplier and the unit price to ``quantity``."""
    return (
        pl.when(pl.col("overtime") == Overtime.normal_hour_text)
        .then(quantity * NORMAL_HOUR * pl.col("unit_price"))
        .when(pl.col("overtime") == Overtime.overtime_150_text)
        .then(quantity * OVERTIME_150 * pl.col("unit_price"))
        .otherwise(quantity * OVERTIME_200 * pl.col("unit_price"))
    )


def _coalesce_fallbacks(column: str) -> pl.Expr:
    """First non-null value of ``column`` across the primary and fallback joins."""
    return pl.coalesce([pl.col(f"{column}{suffix}") for suffix in _FALLBACK_SUFFIXES])


# --------------------------------------------------------------------------- #
# Sources
# --------------------------------------------------------------------------- #


@lru_cache(maxsize=1)
def _scow_transfer_price() -> pl.LazyFrame:
    return get_price_by_type(service_type=ServiceType.cold_store).filter(
        pl.col("service").eq(pl.lit(SCOW_TRANSFER_SERVICE))
    )


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
            pl.col("overtime_tonnage")
            .cast(pl.Utf8)
            .str.replace("", "0")
            .cast(pl.Float64)
            .round(3),
        )
    )


@lru_cache(maxsize=8)
def _bin_dispatch(*group_keys: str) -> pl.LazyFrame:
    """Tonnage totals from the bin dispatch sheet, grouped by ``group_keys``."""
    return (
        _bin_dispatch_base()
        .group_by(list(group_keys))
        .agg(pl.col("total_tonnage").sum(), pl.col("overtime_tonnage").sum())
        .with_columns(
            normal_tonnage=(pl.col("total_tonnage") - pl.col("overtime_tonnage"))
            .round(3)
            .cast(pl.Float64)
        )
    )


# --------------------------------------------------------------------------- #
# Full scows
# --------------------------------------------------------------------------- #


@lru_cache(maxsize=1)
def _full_scows() -> pl.LazyFrame:
    is_special_day = pl.col("day_name").is_in(SPECIAL_DAYS)
    normal_hours = (pl.col("overtime") == Overtime.normal_hour_text) | (
        is_special_day & (pl.col("overtime") == Overtime.overtime_150_text)
    )

    return (
        _transport.scow_transfer.filter(pl.col("status") == Status.full)
        .with_columns(
            _normalised_movement_type_expr(),
            _day_name_expr(),
            storage_type=pl.lit("Dry", dtype=pl.Utf8),
        )
        .with_columns(_overtime_expr())
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
        .join(
            _bin_dispatch("date", "customer", "movement_type"),
            on=["date", "customer", "movement_type"],
            how="left",
        )
        .join(
            _bin_dispatch("date", "movement_type"),
            on=["date", "movement_type"],
            how="left",
            suffix="_fb",
        )
        .join(
            _bin_dispatch("date", "customer"),
            on=["date", "customer"],
            how="left",
            suffix="_cfb",
        )
        .with_columns(
            tonnage=pl.when(normal_hours)
            .then(_coalesce_fallbacks("normal_tonnage"))
            .otherwise(_coalesce_fallbacks("overtime_tonnage"))
            .cast(pl.Float64)
        )
        .drop(_TONNAGE_HELPER_COLUMNS, strict=False)
        .sort("date")
        .join_asof(
            _scow_transfer_price(),
            left_on="date",
            right_on="date",
            strategy="backward",
        )
        .with_columns(
            total_price=pl.when(pl.col("customer").is_in(FREE_OF_CHARGE_CUSTOMERS))
            .then(pl.lit(0.0))
            .otherwise(_rated_price_expr(pl.col("tonnage")))
            .round(3)
            .cast(pl.Float64),
        )
        .with_columns(_movement_label_expr("Full"))
        .select(
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
        )
    )


# --------------------------------------------------------------------------- #
# Empty scows
# --------------------------------------------------------------------------- #


@lru_cache(maxsize=1)
def _empty_scows() -> pl.LazyFrame:
    return (
        _transport.scow_transfer.filter(pl.col("status") == Status.empty)
        .with_columns(_normalised_movement_type_expr(), _day_name_expr())
        .with_columns(_overtime_expr())
        .group_by(["day_name", "date", "customer", "movement_type", "overtime"])
        .agg(
            pl.col("time_out").min().alias("start_time"),
            pl.col("time_in").max().alias("end_time"),
            pl.col("num_of_scows").sum(),
        )
        .sort("date")
        .join_asof(
            _scow_transfer_price(),
            left_on="date",
            right_on="date",
            strategy="backward",
        )
        .with_columns(total_price=_rated_price_expr(pl.col("num_of_scows")))
        .with_columns(_movement_label_expr("Empty"))
        # NOTE: the price frame is filtered on a lower-case ``service`` column,
        # so this exclusion may be a no-op. See the review notes.
        .select(pl.exclude("Service"))
    )


# --------------------------------------------------------------------------- #
# Lazy module attributes (PEP 562)
# --------------------------------------------------------------------------- #

_EXPORTS: dict[str, Callable[[], pl.LazyFrame]] = {
    "full_scows": _full_scows,
    "empty_scows": _empty_scows,
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    builder = _EXPORTS.get(name)
    if builder is not None:
        return builder()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted({*globals(), *_EXPORTS})
