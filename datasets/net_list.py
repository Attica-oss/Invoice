"""The main net-list invoicing frame."""

from __future__ import annotations

from datetime import date
from functools import lru_cache

import polars as pl

from datasets import net_list_raw, cargo
from datasets.stuffing import stuffing_type
from datasets.truck_to_cold_store import cccs_adjusted_records
from datasets.price import unloading_price
from utils import CURRENT_YEAR,apply_overtime_rate,iot_soc


def _non_cccs_operations() -> pl.LazyFrame:
    """Current-year net-list rows, excluding CCCS (which arrive via the
    adjusted-records pipeline)."""
    return (
        net_list_raw()
        .filter(
            pl.col("Date").dt.year().eq(CURRENT_YEAR),
            ~pl.col("Container (Destination)").str.contains("CCCS"),
        )
        .select(
            pl.col("Date").days.add_day_name(),
            pl.col("Date").alias("date"),
            pl.col("Vessel").str.to_uppercase().alias("vessel"),
            pl.col("startTime").alias("start_time"),
            pl.col("Container (Destination)").alias("destination"),
            pl.col("overtime"),
            pl.col("Storage").alias("storage_type"),
            pl.col("endTime").alias("end_time"),
            pl.col("Total Tonnage").round(3).alias("total_tonnage"),
        )
    )


def _service_expr() -> pl.Expr:
    """Classify each operation into a billable service.

    TODO (business rules, carried over unchanged — confirm with ops):
    * The Dardanel (< 2025-09-01) and Asian Marine Reefer
      (2025-11-03..15) date windows are *always false* under the
      CURRENT_YEAR == 2026 filter. Delete them, or widen the year
      filter if 2025 rows are meant to be invoiced here.
    """
    dest = pl.col("destination")
    return (
        pl.when(
            pl.col("stuffing_ox")
            .eq(pl.lit("Basic OSS"))
            .and_(pl.col("vessel_client").is_in(["OCEAN BASKET", "AMIRANTE"]))
        )
        .then(pl.col("stuffing_ox"))
        .when(
            dest.str.contains("IOT")
            | (
                dest.str.contains("DARDANEL")
                # Dardanel loses the discount from 1 Sep 2025
                & pl.col("date").lt(pl.lit(date(2025, 9, 1)))
            )
            | (
                dest.str.contains("Asian Marine Reefer")
                # Asian Marine Reefer billed as IOT in this window
                & pl.col("date").is_between(date(2025, 11, 3), date(2025, 11, 15))
            )
            | dest.str.contains("Unload to Quay")
            | (dest.is_in(iot_soc) & pl.col("customer").eq(pl.lit("IOT")))
        )
        .then(pl.lit("Unload to Quay"))
        .when(dest.str.to_uppercase().is_in(cargo))
        .then(pl.lit("Transhipment"))
        .when(dest.str.contains("CCCS"))
        .then(pl.lit("Unload to CCCS"))
        .otherwise(pl.col("stuffing"))
    )


@lru_cache(maxsize=1)
def net_list() -> pl.LazyFrame:
    """Priced net-list operations (non-CCCS rows + adjusted CCCS rows).

    Pricing changes vs. the old module:
    * The overtime multiplier comes from ``billing.apply_overtime_rate``
      (single shared implementation). The normal-hour multiplier is now
      applied too — a no-op while ``OvertimePerc.normal_hour == 1.0``.
    * Unknown overtime labels get a *null* price instead of silently
      billing at the normal rate. See :func:`netlist_unpriced_rows`.
    """
    return (
        pl.concat(
            [_non_cccs_operations(), cccs_adjusted_records()],
            how="vertical",
        )
        .sort("date")
        # TODO(joins): a forward asof with no tolerance can match a
        # container plugged arbitrarily far in the future; the old
        # tolerance="1d" was commented out. Decide deliberately.
        .join_asof(
            stuffing_type(),
            left_on="date",
            right_on="date_plugged",
            by_left=["destination", "vessel"],
            by_right=["container_number", "vessel_client"],
            strategy="forward",
        )
        .join(
            stuffing_type(),
            left_on=["destination", "date"],
            right_on=["container_number", "date_plugged"],
            how="left",
            suffix="_ox",
        )
        .with_columns(service=_service_expr())
        .drop("stuffing", "customer", "customer_ox", "stuffing_ox", "date_plugged")
        .with_columns(service_storage=pl.col("service") + " - " + pl.col("storage_type"))
        .sort("date")
        .join_asof(
            unloading_price(),
            by_left="service_storage",
            by_right="service",
            on="date",
            strategy="backward",
        )
        .drop("service_storage", "service_type")
        .with_columns(unit_price=apply_overtime_rate())
        .with_columns(
            remarks=pl.when(pl.col("vessel_client").ne(pl.col("vessel")))
            .then(pl.col("vessel_client"))
            .otherwise(None)
        )
        .drop("vessel_client")
        .with_columns(
            invoice_value=(pl.col("unit_price") * pl.col("total_tonnage")).round(3)
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
            "service",
            "unit_price",
            "invoice_value",
            "remarks",
        )
    )


def netlist_unpriced_rows() -> pl.LazyFrame:
    """Net-list rows with a null price — unknown overtime label or no
    matching price entry. These are *excluded from invoice totals* until
    fixed at the source."""
    return net_list().filter(pl.col("unit_price").is_null())
