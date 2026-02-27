"Bin dispatch (Scow Transfer) to and from IOT"

import polars as pl
from polars_result import Result
from price_utils import Overtime, get_price

from dataframe.transport import scow_transfer
from data_source.make_dataset import load_gsheet_data

# Get the miscellaneous data for full scow transfer
from data_source.sheet_ids import (
    MISC_SHEET_ID,
    ALL_CCCS_DATA_SHEET,
)


from type_casting.dates import (
    UPPER_BOUND,
    UPPER_BOUND_SPECIAL_DAY,
    CURRENT_YEAR,
)

from type_casting import (
    ServiceType,
    ContainerStatusType,
    MovementType,
    FishStorageType,
    DayNameType,
    enum_customer,
)


def _price_table() -> pl.DataFrame:
    """
    Fetch price table rows needed for scow transfer.
    Runs only when build_* is called.
    """
    # get_price() is cached via price_table() already (in your data.price module)
    return get_price(["CCCS Movement in/out"])


# Full Scows


def build_bin_dispatch() -> Result[pl.LazyFrame, Exception]:
    """
    Build the bin dispatch dataframe by processing the miscellaneous activity data.

    :return: LazyFrame with bin dispatch data
    :rtype: pl.LazyFrame
    """
    return load_gsheet_data(MISC_SHEET_ID, ALL_CCCS_DATA_SHEET).map(
        lambda lf: (
            lf.filter(
                pl.col("operation_type").is_in(ServiceType.BIN_DISPATCH_SERVICE),
                pl.col("date").dt.year().eq(CURRENT_YEAR),
            )
            .select(
                pl.col("day").cast(DayNameType.enum_dtype()).alias("day_name"),
                pl.col("date"),
                pl.col("movement_type").cast(MovementType.cold_store_perspective()),
                pl.col("customer").cast(enum_customer()),
                pl.col("operation_type").cast(ServiceType.enum_dtype()),
                pl.col("storage_type").cast(FishStorageType.enum_dtype()),
                pl.when(pl.col("movement_type").eq(MovementType.OUT))
                .then(pl.col("bins_out").abs().fill_null(0))
                .otherwise(pl.col("bins_in").fill_null(0))
                .cast(pl.Int64)
                .alias("bins_moved"),
                pl.col("total_tonnage").abs().cast(pl.Float64).round(3),
                pl.col("overtime_tonnage")
                .str.replace("", "0")
                .cast(pl.Float64)
                .round(3),
            )
            .with_columns(
                [
                    (pl.col("total_tonnage") - pl.col("overtime_tonnage"))
                    .round(3)
                    .cast(pl.Float64)
                    .alias("normal_tonnage")
                ]
            )
        )
    )


bin_dispatch = build_bin_dispatch().unwrap()


full_scows: pl.LazyFrame = (
    scow_transfer.filter(pl.col("status").eq(ContainerStatusType.FULL))
    .with_columns(
        [
            pl.when(pl.col("movement_type").eq(MovementType.DELIVERY))
            .then(pl.lit(MovementType.OUT))
            .otherwise(pl.lit(MovementType.IN))
            .cast(dtype=MovementType.enum_dtype())
            .alias("movement_type"),
            pl.lit(FishStorageType.DRY.value, dtype=pl.Utf8).alias("storage_type"),
            pl.col("date").days.add_day_name().alias("day_name"),
        ]
    )
    .with_columns(
        overtime=pl.when(
            (pl.col("day_name").id_special_day()))
            & (pl.col("time_out") > UPPER_BOUND_SPECIAL_DAY)
        )
        .then(pl.lit(Overtime.textual_representation.normal_hours))
        .when(
            (pl.col("day_name").is_in(DAY_NAMES.special_day()))
            | (
                (~pl.col("day_name").is_in(DAY_NAMES.special_day()))
                & (pl.col("time_out") > UPPER_BOUND)
            )
        )
        .then(pl.lit(Overtime.textual_representation.overtime_150))
        .otherwise(pl.lit(Overtime.textual_representation.normal_hours))
    )
    .group_by(
        ["day_name", "date", "customer", "movement_type", "overtime", "storage_type"]
    )
    .agg(
        pl.col("time_out").min().alias("start_time"),
        pl.col("time_in").max().alias("end_time"),
        pl.col("num_of_scows").sum(),
    )
    .join(other=bin_dispatch, on=["date", "customer", "movement_type"], how="left")
    .with_columns(
        tonnage=pl.when(
            (pl.col("overtime") == Overtime.textual_representation.normal_hours)
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
                # "day_name",
                "operation_type",
                "total_tonnage",
                "overtime_tonnage",
                "normal_tonnage",
            ]
        )
    )
    .sort(by=pl.col("date"))
    .join_asof(
        get_price(["CCCS Movement in/out"]).lazy(),
        by=None,
        left_on="date",
        right_on="Date",
        strategy="backward",
    )
    .with_columns(
        total_price=pl.when(
            pl.col("customer").is_in(["ISLAND CATCH", "OCEAN BASKET", "AMIRANTE"])
        )
        .then(pl.lit(0.0))
        .when(pl.col("overtime") == Overtime.textual_representation.normal_hours)
        .then(pl.col("tonnage") * Overtime.normal_hours * pl.col("Price"))
        .when(pl.col("overtime") == Overtime.textual_representation.overtime_150)
        .then(pl.col("tonnage") * Overtime.overtime_150 * pl.col("Price"))
        .otherwise(pl.col("tonnage") * Overtime.overtime_200 * pl.col("Price"))
        .round(3)
        .cast(pl.Float64),
        movement_type=pl.when(pl.col("movement_type") == MovementType.OUT)
        .then(pl.lit("IPHS Delivery of Full Scows to IOT"))
        .when(pl.col("movement_type") == MovementType.IN)
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

# Empty Scows
empty_scows: pl.LazyFrame = (
    scow_transfer.filter(pl.col("status") == Status.empty)
    .with_columns(
        movement_type=pl.when(pl.col("movement_type") == MovementType.delivery)
        .then(pl.lit(MovementType.out))
        .otherwise(pl.lit(MovementType.in_))
        .cast(dtype=MovementType.enum_dtype()),
        day_name=pl.when(pl.col("date").is_in(ph_list))
        .then(pl.lit("PH"))
        .otherwise(pl.col("date").dt.to_string(format="%a")),
    )
    .with_columns(
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
    .group_by(["day_name", "date", "customer", "movement_type", "overtime"])
    .agg(
        pl.col("time_out").min().alias("start_time"),
        pl.col("time_in").max().alias("end_time"),
        pl.col("num_of_scows").sum(),
    )
    .sort(by="date")
    .join_asof(
        SCOW_TRANSFER.lazy(),
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
