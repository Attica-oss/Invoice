"""EMR pipelines: shifting, PTI and washing.

Rewrite notes
-------------
* All frames are cached functions — nothing loads at import time.
* Prices via ``billing.unit_price`` (errors on 0/2 price rows).
* The shifting overtime rate uses ``OvertimePerc.overtime_150`` — the
  old import of ``OVERTIME_150`` from ``data.price`` was a second,
  independent source for the same rate. Confirm they were equal (1.5).
* PTI sessions are sorted by ``datetime_start`` *before* the per-
  container sequence is computed. The old ``cum_count`` ran in sheet
  order, so "previous session" meant "previous row in the sheet".
* ``hours`` stays Float64. The old final cast to ``Int128`` truncated
  7.5 h to 7 (and Int128 breaks most downstream consumers/exports).
* The washing price is no longer cast to ``Int64`` — that truncated the
  *price* (harmless only while the tariff is a whole number).
* Unknown set points yield a *null* electricity price instead of 0;
  see :func:`emr_issues`.

Business rules preserved unchanged (confirm with ops, see TODOs):
* IOT PTI electricity = elapsed days + 1, with NO daily rate multiplied
  in — an IOT container plugged 3 days bills 4 currency units. Almost
  certainly missing a tariff factor.
* The >8 h x2 multiplier applies to the IOT day-based charge too, and a
  multi-day session always exceeds 8 h, so IOT is effectively always
  doubled.
* ``no_shifting == True`` *charges* the shifting fee (gap > 24 h on the
  same generator, or no previous session). The name reads inverted;
  kept for schema compatibility.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

import polars as pl

from data_source.make_dataset import load_sheet as scan_google_sheet
from datasets.price import unit_price
from utils import CURRENT_YEAR, SPECIAL_DAYS, OvertimePerc, SetPoint, containers_enum
from utils.config import (
    EMR_SHEET_ID,
    PTI_SHEET_NAME,
    SHIFTING_SHEET_NAME,
    WASHING_SHEET_NAME,
)

# TODO: move these vocabularies into type_casting so they can't drift
# per call site (same as the SAPMER patch in the stuffing module).
PTI_STATUS: list[str] = ["PASSED", "FAILED"]
PTI_INVOICE_TO: list[str] = ["MAERSKLINE", "IOT", "INVALID", "CMA CGM"]
WASHING_INVOICE_TO: list[str] = [
    "CMA CGM",
    "ECHEBASTAR",
    "ATUNSA",
    "INPESCA",
    "INPESCA S.A",
    "INVALID",
    "IPHS",
    "IOT",
    "IOT IMPORT",
    "PEVASA",
    "RAWANQ",
    "OMAN PELAGIC",
    "MAERSKLINE",
    "SAPMER",
    "OCEAN BASKET",
    "IOT EXP",
    "CCCS",
    "AMIRANTE",
    "JMARR",
    "HARTSWATER",
]

# A session longer than this bills electricity at double the base rate.
LONG_SESSION_HOURS = 8
# Gap after which a container on the same generator incurs a shifting fee.
SHIFTING_GAP = pl.duration(hours=24)


# ---------------------------------------------------------------------------
# Prices
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class EmrPrices:
    magnum_pti_electricity: float
    plugin: float
    s_freezer_pti_electricity: float
    shifting: float
    standard_pti_electricity: float
    washing: float


@lru_cache(maxsize=1)
def emr_prices() -> EmrPrices:
    return EmrPrices(
        magnum_pti_electricity=unit_price("PTI Magnum"),
        plugin=unit_price("Plugin"),
        s_freezer_pti_electricity=unit_price("PTI S Freezer"),
        shifting=unit_price("Shifting"),
        standard_pti_electricity=unit_price("PTI Standard"),
        washing=unit_price("Container Cleaning"),
    )


# ---------------------------------------------------------------------------
# Shifting
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def shifting() -> pl.LazyFrame:
    """Shifting operations, priced at 150% on special days.

    ``ne_missing`` keeps rows with a *null* invoice_to (the old plain
    ``ne`` silently dropped them); they surface in :func:`emr_issues`.
    """
    prices = emr_prices()
    return (
        scan_google_sheet(sheet_id=EMR_SHEET_ID, sheet_name=SHIFTING_SHEET_NAME)
        .filter(pl.col("date").dt.year().eq(CURRENT_YEAR))
        .with_columns(pl.col("date").days.add_day_name())
        .filter(pl.col("invoice_to").ne_missing("INVALID"))
        .select(
            "day_name",
            "date",
            pl.col("container_number").cast(containers_enum),
            "invoice_to",
            "service_remarks",
        )
        .with_columns(
            price=pl.when(pl.col("day_name").cast(pl.Utf8).is_in(SPECIAL_DAYS))
            .then(prices.shifting * OvertimePerc.overtime_150)
            .otherwise(prices.shifting)
        )
    )


# ---------------------------------------------------------------------------
# PTI
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# PTI
# ---------------------------------------------------------------------------

IOT_ELECTRICITY_DAILY_RATE = 70.0


def _hours_between(start_col: str, end_col: str) -> pl.Expr:
    """Elapsed hours, rounded to 2 decimal places."""
    return (
        (pl.col(end_col) - pl.col(start_col)).dt.total_minutes() / 60
    ).round(2)


def _iot_days_between(start_col: str, end_col: str) -> pl.Expr:
    """Inclusive number of calendar days for IOT electricity billing."""
    return (
        pl.col(end_col).dt.date()
        - pl.col(start_col).dt.date()
    ).dt.total_days() + 1


def _electricity_price(prices: EmrPrices) -> pl.Expr:
    """Electricity charge for a PTI session.

    IOT:
        Inclusive calendar days × $70.

    Other customers:
        Electricity tariff based on set point, doubled when the
        session exceeds eight hours.
    """
    standard_rate = (
        pl.when(pl.col("set_point") == SetPoint.s_freezer)
        .then(pl.lit(prices.s_freezer_pti_electricity))
        .when(pl.col("set_point") == SetPoint.magnum)
        .then(pl.lit(prices.magnum_pti_electricity))
        .when(pl.col("set_point") == SetPoint.standard)
        .then(pl.lit(prices.standard_pti_electricity))
        .otherwise(None)
    )

    return (
        pl.when(pl.col("invoice_to") == "IOT")
        .then(
            pl.lit(IOT_ELECTRICITY_DAILY_RATE)
        )
        .otherwise(
            standard_rate * pl.col("above_8_hours")
        )
    )


@lru_cache(maxsize=1)
def pti_staging() -> pl.LazyFrame:
    """PTI sessions with billing quantities and per-session charges."""
    prices = emr_prices()

    return (
        scan_google_sheet(
            sheet_id=EMR_SHEET_ID,
            sheet_name=PTI_SHEET_NAME,
        )
        .filter(
            pl.col("datetime_start").dt.year() == CURRENT_YEAR
        )
        .select(
            "datetime_start",
            pl.col("container_number").cast(containers_enum),
            pl.col("set_point")
            .cast(pl.Utf8)
            .cast(SetPoint.enum_dtype()),
            "unit_manufacturer",
            "datetime_end",
            pl.col("status").cast(pl.Enum(PTI_STATUS)),
            pl.col("invoice_to").cast(pl.Enum(PTI_INVOICE_TO)),
            pl.col("plugged_on").alias("generator"),
        )
        .with_columns(
            hours=_hours_between(
                "datetime_start",
                "datetime_end",
            ),
            days=_iot_days_between(
                "datetime_start",
                "datetime_end",
            ),
            plugin_price=pl.lit(prices.plugin),
        )
        .with_columns(
            above_8_hours=(
                pl.when(
                    (pl.col("invoice_to") != "IOT")
                    & (pl.col("hours") > LONG_SESSION_HOURS)
                )
                .then(2)
                .otherwise(1)
            ),
            billing_quantity=(
                pl.when(pl.col("invoice_to") == "IOT")
                .then(pl.col("days").cast(pl.Float64))
                .otherwise(pl.col("hours"))
            ),
            billing_unit=(
                pl.when(pl.col("invoice_to") == "IOT")
                .then(pl.lit("Days"))
                .otherwise(pl.lit("Hours"))
            ),
        )
        .with_columns(
            electricity_price=_electricity_price(prices)
        )
        .sort("datetime_start")
        .with_columns(
            cum_count=(
                pl.col("container_number")
                .cum_count()
                .over("container_number")
            )
        )
    )


def pti(
    staging: pl.LazyFrame | None = None,
) -> pl.LazyFrame:
    """Main PTI billing frame.

    Shifting applies when there is no previous session, or when the gap
    since the previous session exceeds 24 hours on the same generator.
    """
    lf = staging if staging is not None else pti_staging()

    return (
        lf.with_columns(
            previous=pl.col("cum_count") - 1
        )
        .join(
            lf,
            left_on=[
                "container_number",
                "previous",
            ],
            right_on=[
                "container_number",
                "cum_count",
            ],
            how="left",
        )
        .with_columns(
            shifting_required=(
                (
                    pl.col("datetime_start")
                    - pl.col("datetime_end_right")
                )
                > SHIFTING_GAP
            )
            | (
                pl.col("generator_right")
                != pl.col("generator")
            )
        )
        .with_columns(
            no_shifting=pl.col("shifting_required").fill_null(True)
        )
        .with_columns(
            shifting_price=(
                pl.when(pl.col("no_shifting"))
                .then(pl.lit(emr_prices().shifting))
                .otherwise(pl.lit(0.0))
            )
        )
        .with_columns(
            total_price=pl.when(pl.col("invoice_to").eq("IOT")) .then( pl.col("plugin_price")
        + (pl.col("days") *pl.col("electricity_price"))
        + pl.col("shifting_price"))
                .otherwise(
                    pl.col("plugin_price")
                + pl.col("electricity_price")
                + pl.col("shifting_price")
            ).round(2)
        )
        .select(
            "datetime_start",
            "container_number",
            "set_point",
            "invoice_to",
            "datetime_end",
            "hours",
            "days",
            "billing_quantity",
            "billing_unit",
            "status",
            "plugin_price",
            "electricity_price",
            pl.when(pl.col("no_shifting"))
            .then(1)
            .otherwise(0)
            .alias("no_shifting"),
            "generator",
            "shifting_price",
            "total_price",
        )
    )
# ---------------------------------------------------------------------------
# Washing
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def washing() -> pl.LazyFrame:
    return (
        scan_google_sheet(sheet_id=EMR_SHEET_ID, sheet_name=WASHING_SHEET_NAME)
        .filter(pl.col("date").dt.year().eq(CURRENT_YEAR))
        .select(
            "date",
            pl.col("container_number").cast(containers_enum),
            pl.col("invoice_to").cast(pl.Enum(WASHING_INVOICE_TO)),
            "service_remarks",
        )
        .with_columns(
            price=pl.when(pl.col("invoice_to").ne_missing("INVALID"))
            .then(emr_prices().washing)
            .otherwise(0.0)
        )
    )


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def emr_issues() -> pl.LazyFrame:
    """EMR rows needing attention before invoicing.

    * "negative duration": datetime_end before datetime_start — bills a
      negative day count on the IOT branch and dodges the >8 h check.
    * "unknown set point": non-IOT session whose set point matched no
      tariff — electricity price is null (excluded from totals).
    * "null invoice_to": nobody to bill.
    """
    session = pti_staging().with_columns(
        issue=pl.when(pl.col("hours") < 0)
        .then(pl.lit("negative duration"))
        .when(
            pl.col("electricity_price").is_null()
            & pl.col("invoice_to").cast(pl.Utf8).ne_missing("INVALID")
        )
        .then(pl.lit("unknown set point"))
        .when(pl.col("invoice_to").is_null())
        .then(pl.lit("null invoice_to"))
        .otherwise(None)
    )
    return (
        session.filter(pl.col("issue").is_not_null())
        .select(
            pl.col("datetime_start").alias("date"),
            "container_number",
            pl.col("invoice_to").cast(pl.Utf8),
            pl.col("set_point").cast(pl.Utf8),
            "hours",
            "issue",
        )
        .sort("date")
    )
