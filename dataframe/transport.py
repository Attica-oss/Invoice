"""Transport Lazyframe"""

from datetime import date
from decimal import Decimal
from multiprocessing import set_start_method

import polars as pl
import polars.selectors as cs

from data.price import get_price
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import (
    FORKLIFT_SHEET_NAME,
    SCOW_TRANSFER_SHEET,
    SHORE_CRANE_SHEET_NAME,
    TRANSFER_SHEET_NAME,
    TRANSPORT_SHEET_ID,
)
from type_casting.containers import containers_enum
from type_casting.dates import (
    CURRENT_YEAR,
    LOWER_BOUND,
    MIDNIGHT,
    SPECIAL_DAYS,
    UPPER_BOUND,
    UPPER_BOUND_SPECIAL_DAY,
    DayName,
    Days,
    public_holiday,
)
from type_casting.validations import (
    STATUS_TYPE,
    OvertimePerc,
    ShippingLine,
    Status,
    TransferLocation,
)

ph_list: pl.Series = public_holiday()

# Need to make this as a LazyFrame and do a joinasof incase there is a change in price
SHIFTING_PRICE: float = get_price(["Shifting"]).get_column("unit_price")[0]

SHORE_CRANE_PRICE: float = get_price(["Shore Crane"]).get_column("unit_price")[0]

TRANSFER_PRICE = get_price(["Haulage FEU", "Haulage TEU"])

# Shore crane overtime increase by 10%

INCREASE_10_PERCENT = 1.10
CUT_OFF_DATE = date(2026, 3, 1)

shore_crane: pl.LazyFrame = (
    load_gsheet_data(TRANSPORT_SHEET_ID, SHORE_CRANE_SHEET_NAME)
    .filter(pl.col("date").dt.year().eq(CURRENT_YEAR))
    .select(
        pl.col("day").cast(dtype=DayName.enum_dtype()),
        cs.contains("date"),
        pl.col("start_time"),
        pl.col("end_time"),
        pl.col("hours").dt.hour(),
        pl.col("overtime_hours").dt.hour(),
        pl.col("customer").cast(pl.Utf8),
        pl.col("location").cast(pl.Utf8),
        pl.col("operation_type"),
        pl.col("invoiced_to"),
    )
    .with_columns(
        pl.when(
            (pl.col("day").is_in(SPECIAL_DAYS)).and_(pl.col("date").ge(CUT_OFF_DATE))
        )
        .then(SHORE_CRANE_PRICE * 1.6)
        .when(pl.col("day").is_in(SPECIAL_DAYS))
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
                pl.col("normal_hours").cast(pl.Decimal(precision=3))
                * pl.lit(SHORE_CRANE_PRICE, dtype=pl.Decimal(precision=3))
                * pl.lit(1.6, dtype=pl.Decimal(precision=3))
                # * pl.lit(INCREASE_10_PERCENT, dtype=pl.Decimal(precision=3))
            )
            + (
                pl.col("overtime_hours").cast(pl.Decimal(precision=3))
                * pl.lit(SHORE_CRANE_PRICE, dtype=pl.Decimal(precision=3))
                * pl.lit(2.1, dtype=pl.Decimal(precision=3))
                # * pl.lit(INCREASE_10_PERCENT, dtype=pl.Decimal(precision=3))
            )
        )
        .when(pl.col("day").is_in(SPECIAL_DAYS))
        .then(
            (
                pl.col("normal_hours").cast(pl.Decimal(precision=3))
                * pl.lit(SHORE_CRANE_PRICE, dtype=pl.Decimal(precision=3))
                * pl.lit(OvertimePerc.overtime_150, dtype=pl.Decimal(precision=3))
            )
            + (
                pl.col("overtime_hours").cast(pl.Decimal(precision=3))
                * pl.lit(SHORE_CRANE_PRICE, dtype=pl.Decimal(precision=3))
                * pl.lit(OvertimePerc.overtime_200, dtype=pl.Decimal(precision=3))
            )
        )
        .when(pl.col("date").ge(date(2026, 3, 1)))
        .then(
            (
                pl.col("normal_hours").cast(pl.Decimal(precision=3))
                * pl.lit(SHORE_CRANE_PRICE, dtype=pl.Decimal(precision=3))
                * pl.lit(OvertimePerc.normal_hour, dtype=pl.Decimal(precision=3))
            )
            + (
                pl.col("overtime_hours").cast(pl.Decimal(precision=3))
                * pl.lit(SHORE_CRANE_PRICE, dtype=pl.Decimal(precision=3))
                * pl.lit(1.6, dtype=pl.Decimal(precision=3))
                # * pl.lit(INCREASE_10_PERCENT, dtype=pl.Decimal(precision=3))
            )
        )
        .otherwise(
            (
                pl.col("normal_hours").cast(pl.Decimal(precision=3))
                * pl.lit(SHORE_CRANE_PRICE, dtype=pl.Decimal(precision=3))
                * pl.lit(OvertimePerc.normal_hour, dtype=pl.Decimal(precision=3))
            )
            + (
                pl.col("overtime_hours").cast(pl.Decimal(precision=3))
                * pl.lit(SHORE_CRANE_PRICE, dtype=pl.Decimal(precision=3))
                * pl.lit(OvertimePerc.overtime_150, dtype=pl.Decimal(precision=3))
            )
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
        # pl.col("remarks"),
        pl.col("invoiced_to"),
        pl.col("unit_price"),
        pl.col("total_price"),
    )
    .sort(by=["date", "start_time"])
)

transfer = (
    load_gsheet_data(sheet_id=TRANSPORT_SHEET_ID, sheet_name=TRANSFER_SHEET_NAME)
    .filter(pl.col("date").dt.year().eq(CURRENT_YEAR))
    .sort(["date"])
    .with_columns(
        pl.col("date"),
        pl.col("container_number").cast(dtype=containers_enum),
        pl.col("line").cast(ShippingLine.enum_dtype()),
        pl.col("movement_type").cast(
            dtype=pl.Enum(["Collection", "Shifting", "Delivery"])
        ),
        pl.col("driver").cast(
            dtype=pl.Enum(["NA", "IPHS", "THIRD PARTY", "IPHS (Third Party)"])
        ),
        pl.col("origin").cast(TransferLocation.enum_dtype()),
        pl.col("time_out"),  # .str.to_time(format="%H:%M")
        pl.col("destination").cast(TransferLocation.enum_dtype()),
        pl.col("time_in"),  # .str.to_time(format="%H:%M")
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
    .sort(["service", "date"])
    .join_asof(
        TRANSFER_PRICE.lazy().sort(["service", "date"]),
        by="service",
        right_on="date",
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
        .then(0)
        .when(
            (pl.col("day_name").is_in(SPECIAL_DAYS))
            & (pl.col("time") > UPPER_BOUND_SPECIAL_DAY)
        )
        .then(SHIFTING_PRICE * OvertimePerc.overtime_200)
        .when((pl.col("day_name").is_in(SPECIAL_DAYS)) | (pl.col("time") > UPPER_BOUND))
        .then(SHIFTING_PRICE * OvertimePerc.overtime_150)
        .otherwise(SHIFTING_PRICE),
        haulage_price=pl.when(~pl.col("driver").cast(pl.Utf8).str.contains("IPHS"))
        .then(pl.lit(0))
        .when(
            (pl.col("day_name").is_in(SPECIAL_DAYS))
            & (pl.col("time") > UPPER_BOUND_SPECIAL_DAY)
        )
        .then(pl.col("unit_price") * OvertimePerc.overtime_200)
        .when(
            (
                (pl.col("day_name").is_in(SPECIAL_DAYS))
                | (pl.col("time") > UPPER_BOUND)
            ).or_(pl.col("time").is_between(MIDNIGHT, LOWER_BOUND))
        )
        .then(pl.col("unit_price") * OvertimePerc.overtime_150)
        .otherwise(pl.col("unit_price")),
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
    load_gsheet_data(sheet_id=TRANSPORT_SHEET_ID, sheet_name=SCOW_TRANSFER_SHEET)
    .filter(pl.col("date").dt.year().eq(CURRENT_YEAR))
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
    load_gsheet_data(TRANSPORT_SHEET_ID, FORKLIFT_SHEET_NAME)
    .filter(pl.col("date").dt.year().eq(CURRENT_YEAR))
    .filter(
        ~pl.col("service_type").is_in(["Salt Loading", "Gangway", "Invalid"]),
        pl.col("day") != "",
    )
    .select(
        pl.col("day").cast(dtype=DayName.enum_dtype()),
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
                (pl.col("end_time").gt(UPPER_BOUND)).and_(
                    pl.col("start_time").gt(UPPER_BOUND)
                )
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
        .otherwise(0),
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
        .when(
            (pl.col("day").is_in(SPECIAL_DAYS))
            & (pl.col("end_time") > UPPER_BOUND_SPECIAL_DAY)
        )
        .then(
            (
                pl.col("date").dt.combine(pl.col("end_time"))
                - pl.col("date").dt.combine(UPPER_BOUND_SPECIAL_DAY)
            ).dt.total_minutes()
        )
        .otherwise(0),
    )
    .with_columns(
        normal_hours=(
            pl.col("date").dt.combine(pl.col("end_time"))
            - pl.col("date").dt.combine(pl.col("start_time"))
        ).dt.total_minutes()
        - (pl.col("overtime_150") + pl.col("overtime_200"))
    )
)
