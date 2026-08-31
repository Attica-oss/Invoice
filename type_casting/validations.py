"""Validations"""

from dataclasses import dataclass

import polars as pl

from .polars_enum import PolarsEnum

# Paller types


class PalletType(PolarsEnum):
    """Pallet Type"""

    PALLET = "Pallet"
    LINER_PALLET = "Liner & Pallet"
    LINER = "Liner"


class ShippingLine(PolarsEnum):
    """Shipping Line"""

    MAERSK = "MAERSK"
    CMA_CGM = "CMA CGM"
    IOT = "IOT"


class TransferLocation(PolarsEnum):
    """transfer location"""

    FISHING_PORT = "FISHING PORT"
    IPHS = "IPHS"
    IOT = "IOT"
    HD_YARD = "HD YARD"
    JHL = "JHL"
    LML = "LML"
    CCCS = "CCCS"


# Miscellaneous / CCCS Metrics

# Services
BIN_DISPATCH_SERVICE: list[str] = ["Bin Dispatch to IOT", "Bin Dispatch from IOT"]
UNLOADING_SERVICE: list[str] = ["Sorting from Unloading", "Unsorted from Unloading"]
CARGO_DISPATCH_SERVICE: list[str] = [
    "Dispatch to Cargo Vessel",
    "Dispatch from Cargo Vessel",
]


class MovementType(PolarsEnum):
    """Classification of Movement"""

    delivery = "Delivery"
    collection = "Collection"
    shifting = "Shifting"  # Not used
    external = "External"
    in_ = "IN"
    out = "OUT"
    internal = "INTERNAL"


# Movement Type -> CCCS' Perspective

MOVEMENT_TYPE: list[str] = [MovementType.in_, MovementType.out]


@dataclass
class Status:
    """Check if Full or Empty"""

    full: str = "Full"
    empty: str = "Empty"


STATUS_TYPE: list[str] = [Status.full, Status.empty]


@dataclass
class FishStorage:
    """Brine or Dry"""

    brine: str = "Brine"
    dry: str = "Dry"


FISH_STORAGE: list[str] = [FishStorage.brine, FishStorage.dry]


# Overtime Labels
@dataclass
class Overtime:
    """Overtime Labels"""

    overtime_150_text: str = "overtime 150%"
    overtime_200_text: str = "overtime 200%"
    normal_hour_text: str = "normal hours"


class OvertimePerc:
    """Overtime Percentage rates"""

    overtime_150 = 1.5
    overtime_200 = 2.0
    normal_hour = 1.0


# Stuffing Validations


@dataclass
class PluggedStatus:
    """Check if the unit is Full, Partial of has been Completed"""

    partial: str = "Partial"
    completed: str = "Completed"
    full: str = "Full"


PLUGGED_STATUS: list[str] = [
    PluggedStatus.full,
    PluggedStatus.partial,
    PluggedStatus.completed,
]


class SetPoint(PolarsEnum):
    """Classifies the main 3 set point"""

    standard = "-25"
    magnum = "-35"
    s_freezer = "-60"


# ---------------------------------------------------------------------------
# Shared billing expressions
# ---------------------------------------------------------------------------
# One implementation of the overtime rate multiplier, replacing the
# copy-pasted (and mutually inconsistent) versions that used to live in the
# netlist module. Import this from every pipeline that prices by overtime
# band so the rates can never diverge again.
#
# Fail-loud policy: an overtime label that isn't one of the three known
# values yields a *null* multiplier, which nulls the price. The old code
# billed unknown labels at the normal rate, the overtime rate, or zero --
# three different silent wrong answers. Use ``unknown_overtime_rows`` in a
# validation step to surface them instead.


def overtime_rate_multiplier(overtime_col: str = "overtime") -> pl.Expr:
    """Rate multiplier for the overtime band; null for unknown labels."""
    overtime = pl.col(overtime_col)
    return (
        pl.when(overtime == Overtime.normal_hour_text)
        .then(pl.lit(OvertimePerc.normal_hour))
        .when(overtime == Overtime.overtime_150_text)
        .then(pl.lit(OvertimePerc.overtime_150))
        .when(overtime == Overtime.overtime_200_text)
        .then(pl.lit(OvertimePerc.overtime_200))
        .otherwise(None)
    )


def apply_overtime_rate(
    price_col: str = "unit_price",
    overtime_col: str = "overtime",
) -> pl.Expr:
    """Unit price adjusted by the overtime multiplier (null if unknown)."""
    return pl.col(price_col) * overtime_rate_multiplier(overtime_col)


def unknown_overtime_rows(
    lf: pl.LazyFrame, overtime_col: str = "overtime"
) -> pl.LazyFrame:
    """Rows whose overtime label isn't recognized -- these have null
    prices in the billing frames and must be fixed at the source."""
    return lf.filter(overtime_rate_multiplier(overtime_col).is_null())
