"""Containers List to create enums for type safety"""

from __future__ import annotations

import polars as pl

from scan_google_sheet import scan_google_sheet
from utils.config import TRANSFER_SHEET_NAME, TRANSPORT_SHEET_ID
from utils.validations import MovementType


def filter_containers(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Keep only the rows/cols we need."""
    return (
        lf.filter(pl.col("movement_type").ne(MovementType.delivery))
        .select(pl.col("container_number"), pl.col("line"))
        .sort("container_number")
    )


def load_containers() -> pl.LazyFrame:
    """Loads the containers data as a collected Polars DataFrame."""
    return scan_google_sheet(
        sheet_id=TRANSPORT_SHEET_ID, sheet_name=TRANSFER_SHEET_NAME
    ).pipe(filter_containers)


# Single network call — reuse throughout
_containers_df: pl.LazyFrame = load_containers()

container_list: list[str] = (
    _containers_df.collect().get_column("container_number").unique().to_list()
)

iot_soc: list[str] = (
    _containers_df.filter(pl.col("line").eq("IOT"))
    .collect()
    .get_column("container_number")
    .unique()
    .to_list()
)

containers_enum: pl.Enum = pl.Enum(container_list)
iot_soc_enum: pl.Enum = pl.Enum(iot_soc)
