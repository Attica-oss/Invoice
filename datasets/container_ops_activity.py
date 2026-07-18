
"""Stuffing pipelines: plug-in (COA) billing and pallet/liner charges.

Rewrite notes
-------------
* ``coa`` and ``pallet`` are now cached functions — nothing loads at
  import time. Downstream: ``from dataframe.stuffing import coa`` and
  use ``coa()``.
* ``days_on_plug`` no longer mixes a ``timedelta`` literal with Int64
  branches (a schema error at collect time). Day counts are computed as
  honest calendar days on Date columns via ``dt.total_days()`` — the
  old ``total_hours()/24 -> cast(Int64)`` was an obfuscated identical
  value on Dates, and a truncation bug waiting to happen on datetimes.
* Rows with a null ``date_out`` (still plugged) get an explicit zero
  instead of a null that poisoned ``total``. Rows where ``location``
  and ``date_out`` disagree surface in :func:`stuffing_issues`.
* ``str.contains(..., literal=True)`` — the old ``strict=True`` was the
  wrong keyword (``strict`` governs regex compile errors, not literal
  matching).
* Prices come from ``billing.unit_price`` (errors on 0 or 2 price rows
  instead of grabbing whichever came first).

Billing rules preserved as-is (confirm with ops, see TODOs):
* Completed containers bill ``days + 1`` of electricity; partially
  stuffed ("For Completion") bill ``days``. Asymmetric but assumed
  intentional (the final day is billed on completion).
* Liners are billed only on CMA CGM containers; a liner remark on any
  other shipping line is billed at zero — such rows now appear in
  :func:`stuffing_issues` instead of vanishing.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

import polars as pl

from datasets import unit_price,enum_customer,shipper,shipping_line
from scan_google_sheet import scan_google_sheet
from utils.config import (
    LINER_PALLET_SHEET_NAME,
    PLUGIN_SHEET_NAME,
    STUFFING_SHEET_ID,
)
from utils import (
    CURRENT_YEAR,
    PLUGGED_STATUS,
    PalletType,
    containers_enum
)

# Set points (degrees C) that select the electricity tariff.
# TODO: consider banding (e.g. <= -50 -> super freezer) — a sheet value
# of -59 or -34.5 currently falls through to the *standard* rate.
SET_POINT_S_FREEZER = -60
SET_POINT_MAGNUM = -35


# ---------------------------------------------------------------------------
# Prices
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StuffingPrices:
    liner: float
    magnum_electricity: float
    monitoring: float
    pallet_iot: float
    pallet: float
    plugin: float
    s_freezer_electricity: float
    standard_electricity: float


@lru_cache(maxsize=1)
def stuffing_prices() -> StuffingPrices:
    return StuffingPrices(
        liner=unit_price("Plastic Liner Installation"),
        magnum_electricity=unit_price("Electricity Price Magnum"),
        monitoring=unit_price("Monitoring"),
        pallet_iot=unit_price("Pallets(+ Wedges) Usage"),
        pallet=unit_price("Pallets"),
        plugin=unit_price("Plugin"),
        s_freezer_electricity=unit_price("Electricity Price S Freezer"),
        standard_electricity=unit_price("Electricity Price Standard"),
    )


# ---------------------------------------------------------------------------
# Predicates
# ---------------------------------------------------------------------------

transfer_direct: pl.Expr = pl.col("operation_type").str.contains("Direct")
exchange_hands: pl.Expr = pl.col("operation_type").str.contains("Exchange")

on_plug: pl.Expr = pl.col("location").eq("On Plug")
partially_stuffed: pl.Expr = pl.col("location").eq("For Completion")
plugged_only: pl.Expr = pl.col("location").eq("Plugin Only")
on_plug_or_partially_stuffed: pl.Expr = on_plug | partially_stuffed

still_plugged: pl.Expr = pl.col("date_out").is_null()

# Whole calendar days between plug-in and plug-out (Date columns).
_calendar_days: pl.Expr = (
    (pl.col("date_out") - pl.col("date_plugged")).dt.total_days().cast(pl.Int64)
)


# ---------------------------------------------------------------------------
# Pallet & liner charges
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def load_pallet_dataset() -> pl.LazyFrame:
    """Raw pallet/liner sheet for the current year."""
    return (
        scan_google_sheet(sheet_id=STUFFING_SHEET_ID, sheet_name=LINER_PALLET_SHEET_NAME)
        .filter(pl.col("date").dt.year().eq(CURRENT_YEAR))
        .select(
            pl.col("date"),
            pl.col("container_number").cast(containers_enum),
            # TODO: "SAPMER" is patched into the vocabulary here but not
            # in the plug-in sheet's cast below — if it's a real
            # shipping line, add it to type_casting.shipping_line once.
            pl.col("shipping_line").cast(pl.Enum(shipping_line + ["SAPMER"])),
            pl.col("assigned_to").str.to_uppercase(),
            pl.col("remarks"),
        )
    )


def _remark_contains(word: str) -> pl.Expr:
    # Null remarks yield null -> the when-condition fails -> priced 0.
    return pl.col("remarks").cast(pl.Utf8).str.contains(word, literal=True)


@lru_cache(maxsize=1)
def pallet() -> pl.LazyFrame:
    """Pallet and liner charges per container."""
    prices = stuffing_prices()
    is_iot = pl.col("shipping_line").eq(pl.lit("IOT"))
    is_cma_cgm = pl.col("shipping_line").eq(pl.lit("CMA CGM"))

    return load_pallet_dataset().with_columns(
        pallet_price=pl.when(_remark_contains("Pallet") & is_iot)
        .then(prices.pallet_iot)
        .when(_remark_contains("Pallet"))
        .then(prices.pallet)
        .otherwise(0),
        # Business rule: liners are billed to CMA CGM only. Liner
        # remarks on other lines are billed 0 — see stuffing_issues().
        liner_price=pl.when(_remark_contains("Liner") & is_cma_cgm)
        .then(prices.liner)
        .otherwise(0),
    )


# ---------------------------------------------------------------------------
# Plug-in (COA) billing
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def coa() -> pl.LazyFrame:
    """Plug-in records with electricity, plug-in and monitoring charges.

    Day-count rule:
    * Direct transfers, "On Plug", "Plugin Only": 0 days.
    * "For Completion" (partially stuffed): calendar days.
    * Completed containers: calendar days + 1 (final day billed on
      completion).
    * Still plugged (null ``date_out``): 0 days for now; billed when the
      container leaves. Rows whose ``location`` doesn't say so are
      flagged in :func:`stuffing_issues`.
    """
    prices = stuffing_prices()

    return (
        scan_google_sheet(sheet_id=STUFFING_SHEET_ID, sheet_name=PLUGIN_SHEET_NAME)
        .filter(
            pl.col("date_out").dt.year().eq(CURRENT_YEAR) | pl.col("date_out").is_null()
        )
        .select(
            pl.col("vessel_client").str.to_uppercase().cast(enum_customer()),
            pl.col("customer").cast(pl.Enum(shipper)),
            pl.col("date_plugged"),
            pl.col("time_plugged"),  # TODO: unused — bill by elapsed time?
            pl.col("container_number").cast(containers_enum),
            pl.col("operation_type"),
            pl.col("shipping_line").cast(pl.Enum(shipping_line)),
            pl.col("plugged_status").cast(pl.Enum(PLUGGED_STATUS)),
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
            days_on_plug=pl.when(
                still_plugged | transfer_direct | on_plug | plugged_only
            )
            .then(pl.lit(0, dtype=pl.Int64))
            .when(partially_stuffed)
            .then(_calendar_days)
            .otherwise(_calendar_days + 1),
            plugin_price=pl.when(transfer_direct | exchange_hands)
            .then(0)
            .otherwise(prices.plugin),
            monitoring_price=pl.when(
                still_plugged
                | transfer_direct
                | on_plug_or_partially_stuffed
                | plugged_only
            )
            .then(0)
            .otherwise(prices.monitoring),
        )
        .with_columns(
            electricity_unit_price=pl.when(plugged_only)
            .then(pl.lit(0.0))
            .when(pl.col("set_point").eq(SET_POINT_S_FREEZER))
            .then(prices.s_freezer_electricity)
            .when(pl.col("set_point").eq(SET_POINT_MAGNUM))
            .then(prices.magnum_electricity)
            .otherwise(prices.standard_electricity)
        )
        .with_columns(
            total_electricity=pl.col("electricity_unit_price")
            * pl.col("days_on_plug")
        )
        .with_columns(
            total=pl.col("plugin_price")
            + pl.col("monitoring_price")
            + pl.col("total_electricity")
        )
    )


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def stuffing_issues() -> pl.LazyFrame:
    """Rows needing attention before invoicing.

    * "negative days": date_out before date_plugged — data-entry error,
      currently produces a negative electricity charge.
    * "still plugged, unexpected location": null date_out but location
      isn't one of the still-on-plug states — billed 0 days on trust of
      the location column alone.
    * "liner on non-CMA CGM line": liner installed but billed at zero
      under the CMA-CGM-only rule.
    * "missing set_point": billed at the standard electricity rate by
      fallback, which may undercharge a freezer container.
    """
    plug_issues = (
        coa()
        .with_columns(
            issue=pl.when(pl.col("date_out") < pl.col("date_plugged"))
            .then(pl.lit("negative days"))
            .when(
                still_plugged & ~(on_plug | partially_stuffed | plugged_only)
            )
            .then(pl.lit("still plugged, unexpected location"))
            .when(pl.col("set_point").is_null() & ~plugged_only)
            .then(pl.lit("missing set_point"))
            .otherwise(None)
        )
        .filter(pl.col("issue").is_not_null())
        .select(
            "date_plugged",
            "date_out",
            "container_number",
            pl.col("shipping_line").cast(pl.Utf8),
            "location",
            "issue",
        )
    )

    liner_issues = (
        pallet()
        .filter(
            _remark_contains("Liner")
            & pl.col("shipping_line").cast(pl.Utf8).ne("CMA CGM")
        )
        .select(
            pl.col("date").alias("date_plugged"),
            pl.lit(None, dtype=pl.Date).alias("date_out"),
            "container_number",
            pl.col("shipping_line").cast(pl.Utf8),
            pl.lit(None, dtype=pl.Utf8).alias("location"),
            issue=pl.lit("liner on non-CMA CGM line"),
        )
    )

    return pl.concat([plug_issues, liner_issues], how="vertical").sort("date_plugged")


ELECTRICITY_DATASET = coa()
PALLET_DATASET = pallet().filter(pl.col("remarks").str.contains("Pallet")).select(pl.col("*").exclude(["liner_price"]))
LINER_DATASET = pallet().filter(pl.col("remarks").str.contains("Liner")).select(pl.col("*").exclude(["pallet_price"]))
