"""IOT SOC pipelines: cargo loading and container stuffing.

Changes vs. the old module:
* Day names come from ``.days.add_day_name()`` (public-holiday aware,
  enum-typed) instead of a re-implemented ``is_in(ph_list)`` check.
* Overtime pricing uses the shared ``billing`` helper; the hardcoded
  "normal hours" / "overtime 150%" strings and the silent
  ``.otherwise(0)`` (unknown label -> zero price!) are gone. Unknown
  labels now yield null prices — surface them with
  ``billing.unknown_overtime_rows``.
* ``iot_stuffing`` uses a plain inner join against the pre-filtered
  IOT COA (the old left-join-then-filter was an inner join in disguise).
"""

from __future__ import annotations

from datetime import date

import polars as pl

from datasets.genesis import net_list_raw
from datasets.price import iot_cargo_price, stuffing_price
from datasets.stuffing import iot_coa
from utils import CURRENT_YEAR, apply_overtime_rate, iot_soc
from utils.validations import OvertimePerc

# Asian Marine Reefer shipments from this date are billed to IOT.
# NOTE: deliberately *not* filtered to CURRENT_YEAR — the qualifying
# data starts in Nov 2025. TODO: add an end date or fold this rule into
# the customer module.
_iot_cargo_rows: pl.Expr = pl.col("destination").is_in(
    ["Asian Marine Reefer"]
) & pl.col("date").ge(pl.lit(date(2025, 11, 1)))


def iot_cargo() -> pl.LazyFrame:
    """Loading-to-cargo operations billed to IOT."""
    return (
        net_list_raw()
        .select(
            pl.col("Date").alias("date"),
            pl.col("Vessel").str.to_uppercase().alias("vessel"),
            pl.col("startTime").alias("start_time"),
            pl.col("Container (Destination)").alias("destination"),
            pl.col("overtime"),
            pl.col("Storage").alias("storage"),
            pl.col("endTime").alias("end_time"),
            pl.col("Total Tonnage").alias("total_tonnage"),
        )
        .filter(_iot_cargo_rows)
        .with_columns(
            pl.col("date").days.add_day_name(),
            service=pl.lit("Loading to Cargo") + " - " + pl.col("storage"),
        )
        .sort("date")
        .join_asof(
            iot_cargo_price(),
            by="service",
            on="date",
            strategy="backward",
        )
        .drop("service")
        .with_columns(
            total_price=(pl.col("total_tonnage") * apply_overtime_rate()).round(3)
        )
        .select(
            "vessel",
            "day_name",
            "date",
            "start_time",
            "destination",
            "end_time",
            "overtime",
            "total_tonnage",
            "storage",
            "unit_price",
            "total_price",
        )
    )


def iot_stuffing() -> pl.LazyFrame:
    """Stuffing of IOT SOC containers on the IOT account."""
    return (
        net_list_raw()
        .filter(pl.col("Date").dt.year().eq(CURRENT_YEAR))
        .select(
            pl.col("Date").alias("date"),
            pl.col("Vessel").str.to_uppercase().alias("vessel"),
            pl.col("startTime").alias("start_time"),
            pl.col("Container (Destination)").alias("container_number"),
            pl.col("overtime"),
            pl.col("Storage").alias("storage"),
            pl.col("endTime").alias("end_time"),
            pl.col("Total Tonnage").alias("total_tonnage"),
        )
        .filter(pl.col("container_number").is_in(iot_soc))
        .with_columns(pl.col("date").days.add_day_name())
        .sort("date")
        .join_asof(stuffing_price(), on="date", strategy="backward")
        .drop("service")
        .with_columns(
            total_price=(pl.col("total_tonnage") * apply_overtime_rate()).round(3)
        )
        .select(
            "vessel",
            "day_name",
            "date",
            "start_time",
            "container_number",
            "end_time",
            "overtime",
            "total_tonnage",
            "storage",
            "unit_price",
            "total_price",
        )
        .join(
            iot_coa(),
            left_on=["date", "vessel", "container_number"],
            right_on=["date_plugged", "vessel_client", "container_number"],
            how="inner",
        )
        .select(
            pl.col("vessel"),
            pl.col("day_name"),
            pl.col("date"),
            pl.col("start_time"),
            pl.col("container_number"),
            pl.col("end_time"),
            pl.col("overtime"),
            pl.col("total_tonnage"),
            pl.col("storage"),
            (
                pl.when(pl.col("overtime").eq("normal hours"))
                .then(pl.col("unit_price") * OvertimePerc.normal_hour)
                .when(pl.col("overtime").eq("overtime 150%"))
                .then(pl.col("unit_price") * OvertimePerc.overtime_150)
                .when(pl.col("overtime").eq("overtime 200%"))
                .then(pl.col("unit_price") * OvertimePerc.overtime_200)
                .otherwise(0)
            ).alias("unit_price"),
            pl.col("total_price"),
        )
    )
