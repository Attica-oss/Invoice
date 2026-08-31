"""Haulage transfer dataset with overtime-adjusted shifting and haulage prices."""

from __future__ import annotations

from functools import lru_cache

import polars as pl

from data_source.make_dataset import load_sheet as scan_google_sheet
from data_source.sheet_ids import TRANSFER_SHEET_NAME, TRANSPORT_SHEET_ID
from datasets.price import get_price, unit_price
from type_casting import (
    CURRENT_YEAR,
    MIDNIGHT,
    SPECIAL_DAYS,
    UPPER_BOUND,
    UPPER_BOUND_SPECIAL_DAY,
    OvertimePerc,
    containers_enum,
)
from type_casting.dates import LOWER_BOUND
from type_casting.validations import ShippingLine, Status, TransferLocation


@lru_cache(maxsize=1)
def transfer_price() -> pl.LazyFrame:
    return get_price(["Haulage FEU", "Haulage TEU"]).sort("service", "date").lazy()


@lru_cache(maxsize=1)
def transfer() -> pl.LazyFrame:
    shifting_price = unit_price("Shifting")

    return (
        scan_google_sheet(sheet_id=TRANSPORT_SHEET_ID, sheet_name=TRANSFER_SHEET_NAME)
        .filter(pl.col("date").dt.year().eq(CURRENT_YEAR))
        .sort("date")
        .with_columns(
            pl.col("container_number").cast(containers_enum),
            pl.col("line").cast(ShippingLine.enum_dtype()),
            pl.col("movement_type").cast(pl.Enum(["Collection", "Shifting", "Delivery"])),
            pl.col("driver").cast(pl.Enum(["NA", "IPHS", "THIRD PARTY", "IPHS (Third Party)"])),
            pl.col("origin").cast(TransferLocation.enum_dtype()),
            pl.col("destination").cast(TransferLocation.enum_dtype()),
            pl.col("status").cast(pl.Enum(["Full", "Empty"])),
        )
        .select(pl.all().exclude("invoice_to"))
        .with_columns(
            day_name=pl.col("date").days.add_day_name(),
            service=pl.when(pl.col("size") == "40'")
            .then(pl.lit("Haulage FEU"))
            .when(pl.col("size") == "20'")
            .then(pl.lit("Haulage TEU"))
            .otherwise(pl.lit("Err")),
            time=pl.when(pl.col("movement_type") == "Collection")
            .then(pl.col("time_in"))
            .when(pl.col("movement_type") == "Delivery")
            .then(pl.col("time_out"))
            .otherwise(pl.time()),
        )
        .sort("service", "date")
        .join_asof(
            transfer_price(),
            by="service",
            on="date",
            strategy="backward",
        )
        .with_columns(
            shifting_price=pl.when(
                (
                    (pl.col("type") == "Reefer")
                    & (pl.col("remarks") != "IOT")
                    & (pl.col("status") == Status.full)
                )
                | (pl.col("remarks").eq("CCCS") & pl.col("driver").eq(pl.lit("NA")))
            )
            .then(0)
            .when(
                pl.col("day_name").is_in(SPECIAL_DAYS) & (pl.col("time") > UPPER_BOUND_SPECIAL_DAY)
            )
            .then(shifting_price * OvertimePerc.overtime_200)
            .when(pl.col("day_name").is_in(SPECIAL_DAYS) | (pl.col("time") > UPPER_BOUND))
            .then(shifting_price * OvertimePerc.overtime_150)
            .otherwise(shifting_price),
            haulage_price=pl.when(~pl.col("driver").cast(pl.Utf8).str.contains("IPHS"))
            .then(pl.lit(0))
            .when(
                pl.col("day_name").is_in(SPECIAL_DAYS) & (pl.col("time") > UPPER_BOUND_SPECIAL_DAY)
            )
            .then(pl.col("unit_price") * OvertimePerc.overtime_200)
            .when(
                (pl.col("day_name").is_in(SPECIAL_DAYS) | (pl.col("time") > UPPER_BOUND)).or_(
                    pl.col("time").is_between(MIDNIGHT, LOWER_BOUND)
                )
            )
            .then(pl.col("unit_price") * OvertimePerc.overtime_150)
            .otherwise(pl.col("unit_price")),
        )
        .select(
            "day_name",
            "date",
            "container_number",
            "line",
            "movement_type",
            "driver",
            "origin",
            "time_out",
            "destination",
            "time_in",
            "status",
            "type",
            "size",
            "remarks",
            "shifting_price",
            "haulage_price",
        )
    )


TRANSFER_DATASET = transfer()
