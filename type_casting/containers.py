"""Containers List to create enums for type safety"""

from __future__ import annotations

import polars as pl
from polars_result import Result, Ok, Err

from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import TRANSPORT_SHEET_ID, TRANSFER
from type_casting.validations import MovementType


def lazy_is_empty(lf: pl.LazyFrame) -> bool:
    """Cheap emptiness check for LazyFrame."""
    return lf.limit(1).collect().height == 0


def filter_containers(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Keep only the rows/cols we need."""
    delivery_val = getattr(MovementType.delivery, "value", MovementType.delivery)

    return (
        lf
        .filter(pl.col("movement_type") != delivery_val)
        .select(["container_number", "line"])
        .sort("container_number")
    )


def load_containers() -> Result[pl.LazyFrame, str]:
    """Loads the containers data as a Polars LazyFrame."""
    res = load_gsheet_data(TRANSPORT_SHEET_ID, TRANSFER)  # Result[LazyFrame, str] (assumed)

    match res:
        case Err(e):
            return Err(str(e))
        case Ok(ldf):
            if lazy_is_empty(ldf):
                return Err("No containers found in the data.")
            return Ok(filter_containers(ldf))
        case _:
            return Err(f"Unexpected return type from load_gsheet_data: {type(res)!r}")


# ---- Materialize once (or raise) ----
match load_containers():
    case Ok(ldataf):
        containers_lf = ldataf
    case Err(e):
        raise ValueError(e)

# One collect for everything
containers_df = containers_lf.collect()

container_list: list[str] = containers_df["container_number"].unique().to_list()

iot_soc: list[str] = (
    containers_df
    .filter(pl.col("line") == "IOT")
    .get_column("container_number")
    .unique()
    .to_list()
)

containers_enum: pl.Enum = pl.Enum(container_list)
iot_soc_enum: pl.Enum = pl.Enum(iot_soc)
