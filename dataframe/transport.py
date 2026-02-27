"""Transport Lazyframe"""

from datetime import date

import polars as pl
import polars.selectors as cs

from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import (
    TRANSFER,
    TRANSPORT_SHEET_ID,
    forklift_sheet,
    scow_sheet,
    shore_crane_sheet,
)
from price_utils import Overtime
from price_utils.price import FREE, get_price
from type_casting import DayNameType
from type_casting.containers import containers_enum
from type_casting.dates import (
    LOWER_BOUND,
    MIDNIGHT,
    UPPER_BOUND,
    UPPER_BOUND_SPECIAL_DAY,
    Days,
    public_holiday,
)
from type_casting.validations import ContainerStatusType, ShoreCraneLocationType

ph_list: pl.Series = public_holiday()


def _price_table() -> pl.DataFrame:
    """
    Fetch price table rows needed for scow transfer.
    Runs only when build_* is called.
    """
    # get_price() is cached via price_table() already (in your data.price module)
    return get_price(["Shifting", "Shore Crane", "Haulage FEU", "Haulage TEU"])


# Shore crane overtime increase by 10% starting from 2026-01-01
INCREASE_10_PERCENT = 1.10
CUT_OFF_DATE = date(2026, 1, 1)

shore_crane: pl.LazyFrame = (
    load_gsheet_data(TRANSPORT_SHEET_ID, shore_crane_sheet)
    .unwrap()
    # .filter(pl.col("date").dt.year().eq(CURRENT_YEAR))
    .select(
        pl.col("day").cast(dtype=DayNameType.enum_dtype()).alias("day_name"),
        pl.col("date"),
        pl.col("start_time"),
        pl.col("end_time"),
        pl.col("hours").dt.hour().cast(pl.Int32).alias("hours"),
        pl.col("overtime_hours").dt.hour().cast(pl.Int32).alias("overtime_hours"),
        pl.col("customer").cast(pl.Utf8),
        pl.col("location").cast(ShoreCraneLocationType.enum_dtype()),
        pl.col("operation_type"),
        pl.col("invoiced_to"),
    )
    .with_columns(
        pl.when(
            (pl.col("day_name").is_in(DayNameType.special_day())).and_(
                pl.col("date").ge(CUT_OFF_DATE)
            )
        )
        .then(get_price() * Overtime.overtime_150 * INCREASE_10_PERCENT)
        .when((pl.col("day_name").is_in(DayNameType.special_day())))
        .then(SHORE_CRANE_PRICE * OvertimePerc.overtime_150)
        .otherwise(SHORE_CRANE_PRICE)
        .round(3)
        .alias("unit_price")
    )
    .with_columns((pl.col("hours") - pl.col("overtime_hours")).alias("normal_hours"))
    .with_columns(
        pl.when(pl.col("day").is_in(SPECIAL_DAYS).and_(pl.col("date").ge(CUT_OFF_DATE)))
        .then(
            (
                pl.col("normal_hours")
                * SHORE_CRANE_PRICE
                * OvertimePerc.overtime_150
                * INCREASE_10_PERCENT
            )
            + (
                pl.col("overtime_hours")
                * SHORE_CRANE_PRICE
                * OvertimePerc.overtime_200
                * INCREASE_10_PERCENT
            )
        )
        .when(pl.col("day").is_in(SPECIAL_DAYS))
        .then(
            (pl.col("normal_hours") * SHORE_CRANE_PRICE * OvertimePerc.overtime_150)
            + (pl.col("overtime_hours") * SHORE_CRANE_PRICE * OvertimePerc.overtime_200)
        )
        .when(pl.col("date").ge(date(2026, 1, 1)))
        .then(
            (pl.col("normal_hours") * SHORE_CRANE_PRICE * OvertimePerc.normal_hour)
            + (
                pl.col("overtime_hours")
                * SHORE_CRANE_PRICE
                * OvertimePerc.overtime_150
                * INCREASE_10_PERCENT
            )
        )
        .otherwise(
            (pl.col("normal_hours") * SHORE_CRANE_PRICE * OvertimePerc.normal_hour)
            + (pl.col("overtime_hours") * SHORE_CRANE_PRICE * OvertimePerc.overtime_150)
        )
        .round(3)
        .alias("total_price")
    )
    .select(
        pl.col("day").alias("day_name"),
        pl.col("date"),
        pl.col("start_time"),
        pl.col("end_time"),
        pl.col("hours"),
        pl.col("overtime_hours"),
        pl.col("customer"),
        pl.col("location"),
        pl.col("operation_type"),
        pl.col("remarks"),
        pl.col("invoiced_to"),
        pl.col("unit_price"),
        pl.col("total_price"),
    )
    .sort(by=["date", "start_time"])
)

transfer = (
    load_gsheet_data(TRANSPORT_SHEET_ID, TRANSFER)
    .unwrap()
    .with_columns(
        pl.col("date"),
        pl.col("container_number").cast(dtype=containers_enum),
        pl.col("line"),
        pl.col("movement_type").cast(dtype=pl.Enum(["Collection", "Shifting", "Delivery"])),
        pl.col("driver").cast(dtype=pl.Enum(["NA", "IPHS", "THIRD PARTY", "IPHS (Third Party)"])),
        pl.col("time_out"),  # .str.to_time(format="%H:%M")
        pl.col("time_in"),  # .str.to_time(format="%H:%M")
    )
    .select(pl.all().exclude("invoice_to"))
    .with_columns(
        day_name=pl.col("date").days.add_day_name(),
        Service=pl.when(pl.col("size") == "40'")
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
    .sort(by="date")
    .join_asof(
        TRANSFER_PRICE.lazy(),
        by="Service",
        right_on="Date",
        left_on="date",
        strategy="backward",
    )
    .with_columns(
        shifting_price=pl.when(
            (
                (pl.col("type") == "Reefer")
                & (pl.col("remarks") != "IOT")
                & (pl.col("status") == Status.full)
            )
            | ((pl.col("remarks").eq("CCCS")).and_(pl.col("driver").eq(pl.lit("NA"))))
        )
        .then(FREE)
        .when(
            (pl.col("day_name").is_in(SPECIAL_DAYS)) & (pl.col("time") > UPPER_BOUND_SPECIAL_DAY)
        )
        .then(SHIFTING_PRICE * OvertimePerc.overtime_200)
        .when((pl.col("day_name").is_in(SPECIAL_DAYS)) | (pl.col("time") > UPPER_BOUND))
        .then(SHIFTING_PRICE * OvertimePerc.overtime_150)
        .otherwise(SHIFTING_PRICE),
        haulage_price=pl.when((~pl.col("driver").cast(pl.Utf8).str.contains("IPHS")))
        .then(pl.lit(0))
        .when(
            (pl.col("day_name").is_in(SPECIAL_DAYS)) & (pl.col("time") > UPPER_BOUND_SPECIAL_DAY)
        )
        .then(pl.col("Price") * OvertimePerc.overtime_200)
        .when(
            ((pl.col("day_name").is_in(SPECIAL_DAYS)) | (pl.col("time") > UPPER_BOUND)).or_(
                pl.col("time").is_between(MIDNIGHT, LOWER_BOUND)
            )
        )
        .then(pl.col("Price") * OvertimePerc.overtime_150)
        .otherwise(pl.col("Price")),
    )
    .select(
        [
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
        ]
    )
)


scow_transfer: pl.LazyFrame = (
    load_gsheet_data(TRANSPORT_SHEET_ID, scow_sheet)
    .unwrap()
    .select(
        pl.col("date"),
        pl.col("container_number").cast(dtype=pl.Enum(["STDU6536343", "STDU6536338"])),
        pl.col("customer"),
        pl.col("movement_type"),
        pl.col("driver"),
        pl.col("from"),
        pl.col("time_out"),
        pl.col("destination"),
        pl.col("time_in"),
        pl.col("status").cast(dtype=pl.Enum(STATUS_TYPE)),
        pl.col("remarks"),
        pl.col("num_of_scows").cast(dtype=pl.Int64),
    )
)


forklift: pl.LazyFrame = (
    load_gsheet_data(TRANSPORT_SHEET_ID, forklift_sheet)
    .unwrap()
    .filter(
        ~pl.col("service_type").is_in(["Salt Loading", "Gangway", "Invalid"]),
        pl.col("day") != "",
    )
    .select(
        pl.col("day").cast(dtype=pl.Enum(DAY_NAMES)),
        cs.contains("date"),
        pl.col("start_time"),
        pl.col("end_time"),
        pl.col("duration"),
        pl.col("customer").cast(pl.Utf8),
        pl.col("invoiced_in"),
        pl.col("service_type"),
    )
    .with_columns(
        overtime_150=pl.when(
            (pl.col("day").is_in(SPECIAL_DAYS).not_()).and_(
                (pl.col("end_time").gt(UPPER_BOUND)).and_(pl.col("start_time").gt(UPPER_BOUND))
            )
        )
        .then(
            (
                pl.col("date").dt.combine(pl.col("end_time"))
                - pl.col("date").dt.combine(pl.col("start_time"))
            ).dt.total_minutes()
        )
        .when((~pl.col("day").is_in(SPECIAL_DAYS)) & (pl.col("end_time") > UPPER_BOUND))
        .then(
            (
                pl.col("date").dt.combine(pl.col("end_time"))
                - pl.col("date").dt.combine(UPPER_BOUND)
            ).dt.total_minutes()
        )
        .when(
            (pl.col("day").is_in(SPECIAL_DAYS))
            & (pl.col("end_time") < UPPER_BOUND_SPECIAL_DAY)
            & (pl.col("start_time") < UPPER_BOUND_SPECIAL_DAY)
        )
        .then(
            (
                pl.col("date").dt.combine(pl.col("end_time"))
                - pl.col("date").dt.combine(pl.col("start_time"))
            ).dt.total_minutes()
        )
        .when(
            (pl.col("day").is_in(SPECIAL_DAYS))
            & (pl.col("end_time") > UPPER_BOUND_SPECIAL_DAY)
            & (pl.col("start_time") < UPPER_BOUND_SPECIAL_DAY)
        )
        .then(
            (
                pl.col("date").dt.combine(UPPER_BOUND_SPECIAL_DAY)
                - pl.col("date").dt.combine(pl.col("start_time"))
            ).dt.total_minutes()
        )
        .otherwise(FREE),
        overtime_200=pl.when(
            (pl.col("day").is_in(SPECIAL_DAYS))
            & (
                (pl.col("end_time") > UPPER_BOUND_SPECIAL_DAY)
                & (pl.col("start_time") > UPPER_BOUND_SPECIAL_DAY)
            )
        )
        .then(
            (
                pl.col("date").dt.combine(pl.col("end_time"))
                - pl.col("date").dt.combine(pl.col("start_time"))
            ).dt.total_minutes()
        )
        .when((pl.col("day").is_in(SPECIAL_DAYS)) & (pl.col("end_time") > UPPER_BOUND_SPECIAL_DAY))
        .then(
            (
                pl.col("date").dt.combine(pl.col("end_time"))
                - pl.col("date").dt.combine(UPPER_BOUND_SPECIAL_DAY)
            ).dt.total_minutes()
        )
        .otherwise(FREE),
    )
    .with_columns(
        normal_hours=(
            pl.col("date").dt.combine(pl.col("end_time"))
            - pl.col("date").dt.combine(pl.col("start_time"))
        ).dt.total_minutes()
        - (pl.col("overtime_150") + pl.col("overtime_200"))
    )
)
