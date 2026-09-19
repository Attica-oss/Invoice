"""CCCS tonnage reconciliation.

Genesis scale readings are apportioned between normal and overtime
tonnage using the totals recorded in the miscellaneous-activity sheet,
then re-aggregated per (day, vessel, destination, overtime band).

Fail-loud changes vs. the old module
------------------------------------
* Rows whose overtime label doesn't map to a bucket used to be tagged
  "ERR" and then *billed at the overtime ratio anyway*. They now get a
  null ratio (-> null adjusted tonnage) and appear in
  :func:`cccs_adjusted_issues`.
* Genesis rows with no matching miscellaneous record produced nulls
  that ``sum()`` silently skipped — tonnage vanished from the invoice.
  They also now surface in :func:`cccs_adjusted_issues`.
* Division by a zero window total is guarded.
"""

from __future__ import annotations

from functools import lru_cache

import polars as pl

from datasets.customers import bycatch_companies
from datasets.genesis import genesis_raw
from datasets.miscellaneous import miscellaneous_lf
from type_casting import CURRENT_YEAR, SPECIAL_DAYS, UNLOADING_SERVICE, Overtime

_JOIN_KEYS = ["date", "destination", "vessel", "storage_type"]


@lru_cache(maxsize=1)
def cccs_record() -> pl.LazyFrame:
    """Total and overtime tonnage per (date, destination, vessel,
    storage) from the miscellaneous-activity sheet."""
    return (
        miscellaneous_lf()
        .filter(
            pl.col("operation_type").is_in(UNLOADING_SERVICE),
            ~pl.col("customer").is_in(bycatch_companies),
        )
        .with_columns(
            destination=pl.lit("CCCS (")
            + pl.col("customer")
            .str.replace(" S.A.", "")  # CFTO
            .str.replace(" S.A", "")  # INPESCA
            .str.replace(" SA", "")  # ALBACORA
            .cast(pl.Utf8)
            + pl.lit(")")
        )
        .group_by(_JOIN_KEYS)
        .agg(pl.col("total_tonnage").sum(), pl.col("overtime_tonnage").sum())
        .sort("date")
    )


def _tonnage_bucket() -> pl.Expr:
    """Which side of the misc-sheet split a Genesis row draws from.

    Invariant this encodes (TODO: confirm with ops and document at the
    source): ``overtime_tonnage`` in the miscellaneous sheet counts only
    the 200% band on special days, so special-day 150% rows draw from
    the *normal* side. Branch order matters — the special-day test must
    come before the generic 150% test.
    """
    is_special_day = pl.col("day_name").cast(pl.Utf8).is_in(SPECIAL_DAYS)
    overtime = pl.col("overtime")
    return (
        pl.when(
            (is_special_day & (overtime == Overtime.overtime_150_text))
            | (overtime == Overtime.normal_hour_text)
        )
        .then(pl.lit("normal"))
        .when(
            (overtime == Overtime.overtime_150_text)
            | (overtime == Overtime.overtime_200_text)
        )
        .then(pl.lit("overtime"))
        .otherwise(None)  # unknown label -> surfaces in issues frame
    )


@lru_cache(maxsize=1)
def _adjusted_base() -> pl.LazyFrame:
    """Pre-aggregation frame: one row per Genesis reading with its
    apportionment ratio. Shared by the report and the issues frame."""
    window = ["date", "vessel", "destination", "overtime", "storage_type"]

    return (
        genesis_raw()
        .filter(
            pl.col("Container (Destination)").str.contains("CCCS"),
            pl.col("Date").dt.year().eq(CURRENT_YEAR),
        )
        .select(
            pl.col("Date").days.add_day_name(),
            pl.col("Date").alias("date"),
            pl.col("Time"),
            pl.col("overtime"),
            pl.col("Storage").alias("storage_type"),
            pl.col("Vessel").str.to_uppercase().alias("vessel"),
            (
                pl.col("Scale Reading(-Fish Net) (Cal)")
                .str.replace(",", "")
                .cast(pl.Int64)
                * 0.001  # kilos -> tons
            )
            .round(3)
            .alias("total_tonnage"),
            pl.col("Container (Destination)").alias("destination"),
            pl.col("Species"),
        )
        .with_columns(
            tons=pl.col("total_tonnage").sum().over(window),
            tonnage_bucket=_tonnage_bucket(),
        )
        .join(cccs_record(), on=_JOIN_KEYS, how="left")
        .with_columns(
            normal_tonnage=pl.col("total_tonnage_right") - pl.col("overtime_tonnage")
        )
        .with_columns(
            perc_diff=pl.when(pl.col("tons") <= 0)
            .then(None)  # zero/negative window total: cannot apportion
            .when(pl.col("tonnage_bucket") == "normal")
            .then(pl.col("normal_tonnage") / pl.col("tons"))
            .when(pl.col("tonnage_bucket") == "overtime")
            .then(pl.col("overtime_tonnage") / pl.col("tons"))
            .otherwise(None)
        )
        .with_columns(adjusted_tonnage=pl.col("total_tonnage") * pl.col("perc_diff"))
    )


def cccs_adjusted_records() -> pl.LazyFrame:
    """Adjusted CCCS tonnage per (day, date, overtime, vessel,
    destination, storage), with first/last scale-reading times.

    NOTE: ``Time.min()/max()`` — if the Time column is a string, this is
    a lexicographic min/max ("9:00" > "10:00"). Confirm the loader
    parses it to ``pl.Time``.
    """
    return (
        _adjusted_base()
        .group_by(
            ["day_name", "date", "overtime", "vessel", "destination", "storage_type"]
        )
        .agg(
            start_time=pl.col("Time").min(),
            end_time=pl.col("Time").max(),
            total_tonnage=pl.col("adjusted_tonnage").sum().round(3),
        )
        .select(
            "day_name",
            "date",
            "vessel",
            "start_time",
            "destination",
            "overtime",
            "storage_type",
            "end_time",
            "total_tonnage",
        )
        .sort("date")
    )


def cccs_adjusted_issues() -> pl.LazyFrame:
    """Genesis rows that could not be apportioned, with the reason.

    Any row here is *missing from the invoice totals* — resolve at the
    source (misc sheet entry missing, bad overtime label, or a
    zero-tonnage window)."""
    return (
        _adjusted_base()
        .filter(pl.col("perc_diff").is_null())
        .with_columns(
            issue=pl.when(pl.col("total_tonnage_right").is_null())
            .then(pl.lit("no matching miscellaneous record"))
            .when(pl.col("tonnage_bucket").is_null())
            .then(pl.lit("unknown overtime label"))
            .when(pl.col("tons") <= 0)
            .then(pl.lit("zero-tonnage window"))
            .otherwise(pl.lit("unapportionable"))
        )
        .select(
            "date",
            "vessel",
            "destination",
            "storage_type",
            "overtime",
            "total_tonnage",
            "issue",
        )
        .sort("date")
    )
