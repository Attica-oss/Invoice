"""Container-stuffing classification frames derived from the COA data."""

from __future__ import annotations

from functools import lru_cache

import polars as pl

from datasets.container_ops_activity import coa


@lru_cache(maxsize=1)
def stuffing_type() -> pl.LazyFrame:
    """One row per (vessel_client, container, date_plugged) with the
    stuffing category: Full OSS / Basic OSS / Container Stuffing.

    Sorted by ``date_plugged`` — required by the forward ``join_asof``
    in :mod:`net_list.core`.
    """
    return (
        coa().select(
            "vessel_client",
            "customer",
            "date_plugged",
            "container_number",
            "operation_type",
        )
        .with_columns(
            pl.col("container_number").cast(pl.Utf8),
            pl.col("vessel_client").cast(pl.Utf8),
        )
        .filter(
            ~pl.col("operation_type").str.contains("CCCS"),
            pl.col("operation_type").str.contains_any(["Full", "Basic", "Stuffing"]),
        )
        .with_columns(
            stuffing=pl.when(pl.col("operation_type").str.contains("Full"))
            .then(pl.lit("Full OSS"))
            .when(pl.col("operation_type").str.contains("Basic"))
            .then(pl.lit("Basic OSS"))
            .otherwise(pl.lit("Container Stuffing"))
        )
        .drop("operation_type")
        .unique()
        .sort("date_plugged")
    )


@lru_cache(maxsize=1)
def iot_coa() -> pl.LazyFrame:
    """Containers stuffed on the IOT account (shipping line == IOT),
    restricted to customer == IOT.

    The customer filter lives here so :func:`net_list.iot.iot_stuffing`
    can use a plain inner join (the old code left-joined and then
    filtered, which is an inner join written confusingly).
    """
    return (
        coa().with_columns(
            pl.col("vessel_client").cast(pl.Utf8),
            pl.col("container_number").cast(pl.Utf8),
        )
        .select(
            "vessel_client",
            "customer",
            "operation_type",
            "shipping_line",
            "date_plugged",
            "container_number",
        )
        .filter(
            pl.col("shipping_line").eq(pl.lit("IOT")),
            pl.col("operation_type").str.contains(pl.lit("Stuffing")),
            pl.col("customer").eq(pl.lit("IOT")),
        )
        .drop("operation_type", "shipping_line", "customer")
    )
