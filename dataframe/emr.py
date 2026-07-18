"""EMR lazyframes"""

from __future__ import annotations

import polars as pl

from data.price import OVERTIME_150, get_price
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import (
    EMR_SHEET_ID,
    PTI_SHEET_NAME,
    SHIFTING_SHEET_NAME,
    WASHING_SHEET_NAME,
)
from type_casting.containers import containers_enum
from type_casting.dates import CURRENT_YEAR, SPECIAL_DAYS
from type_casting.validations import SetPoint

# Price

MAGNUM_PTI_ELECTRICITY = (
    get_price(["PTI Magnum"]).select(pl.col("unit_price")).to_series()[0]
)
PLUGIN = get_price(["Plugin"]).select(pl.col("unit_price")).to_series()[0]
S_FREEZER_PTI_ELECTRICITY = (
    get_price(["PTI S Freezer"]).select(pl.col("unit_price")).to_series()[0]
)
SHIFTING = get_price(["Shifting"]).select(pl.col("unit_price")).to_series()[0]
STANDARD_PTI_ELECTRICITY = (
    get_price(["PTI Standard"]).select(pl.col("unit_price")).to_series()[0]
)
WASHING = get_price(["Container Cleaning"]).select(pl.col("unit_price")).to_series()[0]


# Shifting Data Set
shifting: pl.LazyFrame = (
    load_gsheet_data(sheet_id=EMR_SHEET_ID, sheet_name=SHIFTING_SHEET_NAME)
    .filter(pl.col("date").dt.year().eq(CURRENT_YEAR))
    # Add the day name column to invoice overtime calculation
    .with_columns(
        pl.col("date").days.add_day_name()
        # Filter invalid invoices
    )
    .filter(pl.col("invoice_to").ne("INVALID"))
    .select(
        pl.col("day_name"),
        pl.col("date"),
        pl.col("container_number").cast(dtype=containers_enum),
        pl.col("invoice_to"),
        pl.col("service_remarks"),
    )
    .with_columns(
        pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
        .then(SHIFTING * OVERTIME_150)
        .otherwise(SHIFTING)
        .alias("price")
    )
)


# PTI staging Data Set
_pti: pl.LazyFrame = (
    load_gsheet_data(EMR_SHEET_ID, PTI_SHEET_NAME)
    .filter(pl.col("datetime_start").dt.year().eq(CURRENT_YEAR))
    .select(
        pl.col("datetime_start"),
        pl.col("container_number").cast(dtype=containers_enum),
        pl.col("set_point").cast(pl.Utf8).cast(dtype=SetPoint.enum_dtype()),
        pl.col("unit_manufacturer"),
        pl.col("datetime_end"),
        pl.col("status").cast(dtype=pl.Enum(["PASSED", "FAILED"])),
        pl.col("invoice_to").cast(
            dtype=pl.Enum(["MAERSKLINE", "IOT", "INVALID", "CMA CGM"])
        ),
        pl.col("plugged_on").alias("generator"),
    )
    .with_columns(
        [
            (
                (pl.col("datetime_end") - pl.col("datetime_start")).dt.total_minutes()
                / 60
            )
            .round(2)
            .alias("hours"),
            pl.lit(PLUGIN).alias("plugin_price"),
        ]
    )
    .with_columns(
        above_8_hours=pl.when(pl.col("hours").gt(pl.lit(8))).then(2).otherwise(1)
    )
    .with_columns(
        electricity_price=(
            pl.when(pl.col("invoice_to").eq(pl.lit("IOT")))
            .then(
                (pl.col("datetime_end") - pl.col("datetime_start")).dt.total_hours()
                / 24
                + 1
            )
            .when(pl.col("set_point").eq(SetPoint.s_freezer))
            .then(S_FREEZER_PTI_ELECTRICITY)
            .when(pl.col("set_point") == SetPoint.magnum)
            .then(MAGNUM_PTI_ELECTRICITY)
            .when(pl.col("set_point") == SetPoint.standard)
            .then(STANDARD_PTI_ELECTRICITY)
            .otherwise(0)
        )
        * pl.col("above_8_hours")
    )
    .with_columns(
        pl.col("container_number")
        .cum_count()
        .over(pl.col("container_number"))
        .alias("cum_count")
    )
)

# Main PTI Data Set
pti: pl.LazyFrame = (
    _pti.with_columns((pl.col("cum_count") - 1).alias("previous"))
    .join(
        _pti,
        left_on=["container_number", "previous"],
        right_on=["container_number", "cum_count"],
        how="left",
    )
    .with_columns(
        no_shifting=(
            (
                (pl.col("datetime_start") - pl.col("datetime_end_right"))
                > pl.duration(hours=24)
            )
            & (pl.col("generator_right") == pl.col("generator"))
        ).fill_null(True)
    )
    .select(
        pl.col(
            [
                "datetime_start",
                "container_number",
                "set_point",
                "invoice_to",
                "datetime_end",
                "hours",
                "status",
                "plugin_price",
                "electricity_price",
                "no_shifting",
                "generator",
            ]
        )
    )
    .with_columns(
        shifting_price=pl.when(pl.col("no_shifting")).then(SHIFTING).otherwise(0)
    )
    .with_columns(
        (
            pl.col("plugin_price")
            + pl.col("electricity_price")
            + pl.col("shifting_price")
        ).alias("total_price")
    )
    .select(
        pl.col("datetime_start"),
        pl.col("container_number"),
        pl.col("set_point"),
        pl.col("invoice_to"),
        pl.col("datetime_end"),
        pl.col("hours").cast(pl.Int128),
        pl.col("status"),
        pl.col("plugin_price"),
        pl.col("electricity_price"),
        pl.when(pl.col("no_shifting")).then(1).otherwise(0).alias("no_shifting"),
        pl.col("generator"),
        pl.col("shifting_price"),
        pl.col("total_price"),
    )
)

# Washing Data Set
washing = (
    load_gsheet_data(EMR_SHEET_ID, WASHING_SHEET_NAME)
    .filter(pl.col("date").dt.year().eq(CURRENT_YEAR))
    .select(
        pl.col("date"),
        pl.col("container_number").cast(dtype=containers_enum),
        pl.col("invoice_to").cast(
            dtype=pl.Enum(
                [
                    "CMA CGM",
                    "ECHEBASTAR",
                    "ATUNSA",
                    "INPESCA",
                    "INPESCA S.A",
                    "INVALID",
                    "IPHS",
                    "IOT",
                    "IOT IMPORT",
                    "PEVASA",
                    "RAWANQ",
                    "OMAN PELAGIC",
                    "MAERSKLINE",
                    "SAPMER",
                    "OCEAN BASKET",
                    "IOT EXP",
                    "CCCS",
                    "AMIRANTE",
                    "JMARR",
                    "HARTSWATER",
                ]
            )
        ),
        pl.col("service_remarks"),
    )
    .with_columns(
        price=pl.when(pl.col("invoice_to") != "INVALID")
        .then(WASHING)
        .otherwise(0)
        .cast(pl.Int64)
    )
)
