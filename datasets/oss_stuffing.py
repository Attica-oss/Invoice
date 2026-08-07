"""Maersk OSS stuffing list, split between Full and Basic OSS.

BUG FIX vs. the old module: the overtime-adjusted price was computed
into a ``Price`` column that was never used — ``invoice_value`` was
taken from the *unadjusted* unit price, under-invoicing every OSS
stuffing performed during overtime. The adjusted price is now the one
that's billed.
"""

from __future__ import annotations

import polars as pl

from datasets import net_list, oss_stuffing_price
from utils import apply_overtime_rate


def oss() -> pl.LazyFrame:
    return (
        net_list()
        .drop("unit_price", "invoice_value")
        .filter(pl.col("service").str.contains("OSS"))
        .with_columns(
            # Bill the client named in remarks when it differs from the
            # operating vessel.
            vessel=pl.when(pl.col("remarks").is_null().or_(pl.col("service") == pl.lit("Full OSS")))
            .then(pl.col("vessel"))
            .otherwise(pl.col("remarks")),
            service=pl.when(pl.col("service") == pl.lit("Full OSS"))
            .then(pl.lit("Container Stuffing") + " - " + pl.col("storage_type"))
            .otherwise(pl.lit("Stuffing")),
        )
        .sort("date")
        .join_asof(
            oss_stuffing_price(),
            by="service",
            on="date",
            strategy="backward",
        )
        .drop("service_type", "remarks")
        .with_columns(unit_price=apply_overtime_rate())
        .with_columns(
            invoice_value=(pl.col("unit_price") * pl.col("total_tonnage")).round(3)
        ).unique()
    )
