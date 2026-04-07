"""Containers List to create enums for type safety"""

from __future__ import annotations

import polars as pl
from polars.lazyframe.in_process import InProcessQuery

from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import TRANSFER, TRANSPORT_SHEET_ID
from type_casting.validations import MovementType


def filter_containers(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Keep only the rows/cols we need."""
    delivery_val = getattr(MovementType.delivery, "value", MovementType.delivery)
    return (
        lf.filter(pl.col("movement_type") != delivery_val)
        .select(["container_number", "line"])
        .sort("container_number")
    )


def load_containers() -> pl.DataFrame | InProcessQuery:
    """Loads the containers data as a collected Polars DataFrame."""
    return load_gsheet_data(TRANSPORT_SHEET_ID, TRANSFER).pipe(filter_containers).collect()


# Single network call — reuse throughout
_containers_df: pl.DataFrame | InProcessQuery = load_containers()

container_list: list[str] = _containers_df.get_column("container_number").unique().to_list()

iot_soc: list[str] = (
    _containers_df.filter(pl.col("line") == "IOT")
    .get_column("container_number")
    .unique()
    .to_list()
)

containers_enum: pl.Enum = pl.Enum(container_list)
iot_soc_enum: pl.Enum = pl.Enum(iot_soc)
