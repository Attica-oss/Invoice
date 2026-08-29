"""Washing Dataset"""

from functools import lru_cache

import polars as pl

from data import ServiceType, get_price_by_type
from data_source import EMR_SHEET_ID, WASHING_SHEET_NAME, load_gsheet_data

# from dataframe.emr import WASHING
from type_casting import CURRENT_YEAR, containers_enum


@lru_cache(maxsize=1)
def _cleaning_price() -> pl.LazyFrame:
    return (
        get_price_by_type(service_type=ServiceType.depot)
        .filter(pl.col("service").eq("Container Cleaning"))
        .drop(["service", "service_type"])
    )


# Washing Data Set
@lru_cache(maxsize=1)
def washing() -> pl.LazyFrame:
    return (
        load_gsheet_data(sheet_id=EMR_SHEET_ID, sheet_name=WASHING_SHEET_NAME)
        .filter(
            pl.col("date")
            .dt.year()
            .eq(CURRENT_YEAR)
            .and_(pl.col("invoice_to").ne("INVALID"))
        )
        .select(
            pl.col("date"),
            pl.col("container_number").cast(dtype=containers_enum),
            pl.col("invoice_to"),
            pl.col("service_remarks"),
        )
        .sort(by="date")
        .join_asof(
            _cleaning_price(),
            by=None,
            left_on="date",
            right_on="date",
            strategy="backward",
        )
    )
