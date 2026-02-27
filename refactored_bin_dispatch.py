"""Bin dispatch (Scow Transfer) to and from IOT"""

from __future__ import annotations

import polars as pl
from polars_result import Result

from data.price import OVERTIME_150, OVERTIME_200, NORMAL_HOUR, get_price
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import MISC_SHEET_ID, ALL_CCCS_DATA_SHEET

from type_casting.dates import (
    SPECIAL_DAYS,
    UPPER_BOUND,
    UPPER_BOUND_SPECIAL_DAY,
    public_holiday,
    CURRENT_YEAR,
    DAY_NAMES,
)
from type_casting.validations import (
    BIN_DISPATCH_SERVICE,
    Status,
    MovementType,
    Overtime,
)

# IMPORTANT:
# Do NOT import `scow_transfer` as a global LazyFrame if it triggers work at import.
# Instead import its builder (you will need to create it if you haven't yet).
# Example below assumes you refactor transport.py to expose build_scow_transfer().
from dataframe.transport import (
    build_scow_transfer,
)  # <-- you should create this builder


def _price_table() -> pl.DataFrame:
    """
    Fetch price table rows needed for scow transfer.
    Runs only when build_* is called.
    """
    # get_price() is cached via price_table() already (in your data.price module)
    return get_price(["CCCS Movement in/out"])


def build_bin_dispatch() -> Result[pl.LazyFrame, Exception]:
    """
    Loads the bin dispatch tonnage dataset from CCCS report sheet.
    """
    return load_gsheet_data(MISC_SHEET_ID, ALL_CCCS_DATA_SHEET).map(
        lambda lf: (
            lf.filter(
                pl.col("operation_type").is_in(BIN_DISPATCH_SERVICE),
                pl.col("date").dt.year() >= CURRENT_YEAR - 1,
            )
            .select(
                pl.col("day").cast(pl.Enum(DAY_NAMES)).alias("day_name"),
                pl.col("date"),
                pl.col("movement_type").cast(MovementType.enum_dtype()),
                pl.col("customer"),
                pl.col("operation_type"),
                pl.col("total_tonnage").abs().cast(pl.Float64).round(3),
                pl.col("overtime_tonnage")
                .str.replace("", "0")
                .cast(pl.Float64)
                .round(3),
            )
            .with_columns(
                normal_tonnage=(
                    (pl.col("total_tonnage") - pl.col("overtime_tonnage"))
                    .round(3)
                    .cast(pl.Float64)
                )
            )
        )
    )


def _add_overtime_columns(lf: pl.LazyFrame, ph_list: pl.Series) -> pl.LazyFrame:
    """
    Common overtime/day_name logic used by full + empty.
    """
    return lf.with_columns(
        day_name=pl.when(pl.col("date").is_in(ph_list))
        .then(pl.lit("PH"))
        .otherwise(pl.col("date").dt.to_string(format="%a")),
    ).with_columns(
        overtime=pl.when(
            (pl.col("day_name").is_in(SPECIAL_DAYS))
            & (pl.col("time_out") > UPPER_BOUND_SPECIAL_DAY)
        )
        .then(pl.lit(Overtime.overtime_200_text))
        .when(
            (pl.col("day_name").is_in(SPECIAL_DAYS))
            | (
                (~pl.col("day_name").is_in(SPECIAL_DAYS))
                & (pl.col("time_out") > UPPER_BOUND)
            )
        )
        .then(pl.lit(Overtime.overtime_150_text))
        .otherwise(pl.lit(Overtime.normal_hour_text))
    )


def build_full_scows() -> Result[pl.LazyFrame, Exception]:
    """
    Full scows invoice dataset.
    """
    # Build-time values (computed only when this function is called)
    ph_list: pl.Series = public_holiday()
    price_df = _price_table()
    scow_price_lf = price_df.lazy()

    return build_scow_transfer().and_then(
        lambda scow_transfer: build_bin_dispatch().map(
            lambda bin_dispatch: (
                scow_transfer.filter(pl.col("status") == Status.full)
                .with_columns(
                    movement_type=pl.when(
                        pl.col("movement_type") == MovementType.delivery
                    )
                    .then(pl.lit(MovementType.out))
                    .otherwise(pl.lit(MovementType.in_))
                    .cast(dtype=MovementType.enum_dtype()),
                    storage_type=pl.lit("Dry", dtype=pl.Utf8),
                )
                .pipe(_add_overtime_columns, ph_list)
                .group_by(
                    [
                        "day_name",
                        "date",
                        "customer",
                        "movement_type",
                        "overtime",
                        "storage_type",
                    ]
                )
                .agg(
                    pl.col("time_out").min().alias("start_time"),
                    pl.col("time_in").max().alias("end_time"),
                    pl.col("num_of_scows").sum(),
                )
                .join(
                    other=bin_dispatch,
                    on=["date", "customer", "movement_type"],
                    how="left",
                )
                .with_columns(
                    tonnage=pl.when(
                        (pl.col("overtime") == Overtime.normal_hour_text)
                        | (
                            (pl.col("day_name").is_in(SPECIAL_DAYS))
                            & (pl.col("overtime") == Overtime.overtime_150_text)
                        )
                    )
                    .then(pl.col("normal_tonnage"))
                    .otherwise(pl.col("overtime_tonnage"))
                    .cast(pl.Float64)
                )
                .select(
                    pl.all().exclude(
                        [
                            "operation_type",
                            "total_tonnage",
                            "overtime_tonnage",
                            "normal_tonnage",
                        ]
                    )
                )
                .sort(by=pl.col("date"))
                .join_asof(
                    scow_price_lf,
                    by=None,
                    left_on="date",
                    right_on="Date",
                    strategy="backward",
                )
                .with_columns(
                    total_price=pl.when(
                        pl.col("customer").is_in(
                            ["ISLAND CATCH", "OCEAN BASKET", "AMIRANTE"]
                        )
                    )
                    .then(pl.lit(0.0))
                    .when(pl.col("overtime") == Overtime.normal_hour_text)
                    .then(pl.col("tonnage") * NORMAL_HOUR * pl.col("Price"))
                    .when(pl.col("overtime") == Overtime.overtime_150_text)
                    .then(pl.col("tonnage") * OVERTIME_150 * pl.col("Price"))
                    .otherwise(pl.col("tonnage") * OVERTIME_200 * pl.col("Price"))
                    .round(3)
                    .cast(pl.Float64),
                    movement_type=pl.when(pl.col("movement_type") == MovementType.out)
                    .then(pl.lit("IPHS Delivery of Full Scows to IOT"))
                    .when(pl.col("movement_type") == MovementType.in_)
                    .then(pl.lit("IPHS Collection of Full Scows from IOT"))
                    .otherwise(pl.lit("Err")),
                )
                .select(
                    [
                        "day_name",
                        "date",
                        "customer",
                        "movement_type",
                        "overtime",
                        "start_time",
                        "end_time",
                        "num_of_scows",
                        "storage_type",
                        "tonnage",
                        "Price",
                        "total_price",
                    ]
                )
            )
        )
    )


def build_empty_scows() -> Result[pl.LazyFrame, Exception]:
    """
    Empty scows invoice dataset.
    """
    ph_list: pl.Series = public_holiday()
    price_df = _price_table()
    scow_price_lf = price_df.lazy()

    return build_scow_transfer().map(
        lambda scow_transfer: (
            scow_transfer.filter(pl.col("status") == Status.empty)
            .with_columns(
                movement_type=pl.when(pl.col("movement_type") == MovementType.delivery)
                .then(pl.lit(MovementType.out))
                .otherwise(pl.lit(MovementType.in_))
                .cast(dtype=MovementType.enum_dtype()),
            )
            .pipe(_add_overtime_columns, ph_list)
            .group_by(["day_name", "date", "customer", "movement_type", "overtime"])
            .agg(
                pl.col("time_out").min().alias("start_time"),
                pl.col("time_in").max().alias("end_time"),
                pl.col("num_of_scows").sum(),
            )
            .sort(by="date")
            .join_asof(
                scow_price_lf,
                by=None,
                left_on="date",
                right_on="Date",
                strategy="backward",
            )
            .with_columns(
                total_price=pl.when(pl.col("overtime") == Overtime.normal_hour_text)
                .then(pl.col("num_of_scows") * NORMAL_HOUR * pl.col("Price"))
                .when(pl.col("overtime") == Overtime.overtime_150_text)
                .then(pl.col("num_of_scows") * OVERTIME_150 * pl.col("Price"))
                .otherwise(pl.col("num_of_scows") * OVERTIME_200 * pl.col("Price")),
                movement_type=pl.when(pl.col("movement_type") == MovementType.out)
                .then(pl.lit("IPHS Delivery of Empty Scows to IOT"))
                .when(pl.col("movement_type") == MovementType.in_)
                .then(pl.lit("IPHS Collection of Empty Scows from IOT"))
                .otherwise(pl.lit("Err")),
            )
            .select(pl.all().exclude(["Service", "Date"]))
        )
    )
