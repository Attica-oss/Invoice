"""Transport Lazyframe"""

from datetime import date
import polars as pl
import polars.selectors as cs
from data.price import FREE, get_price
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import (
    TRANSPORT_SHEET_ID,
    TRANSFER,
    shore_crane_sheet,
    forklift_sheet,
    scow_sheet,
)
from type_casting.dates import (
    DayName,
    CURRENT_YEAR,
    SPECIAL_DAYS,
    UPPER_BOUND,
    MIDNIGHT,
    LOWER_BOUND,
    UPPER_BOUND_SPECIAL_DAY,
    public_holiday,
    Days,
)
from type_casting.containers import containers_enum
from type_casting.validations import (
    STATUS_TYPE,
    OvertimePerc,
    Status,
    ShippingLine,
    TransferLocation,
)

ph_list: pl.Series = public_holiday()

# Need to make this as a LazyFrame and do a joinasof incase there is a change in price
SHIFTING_PRICE = get_price(["Shifting"]).select(pl.col("Price")).to_series()[0]

SHORE_CRANE_PRICE: float = (
    get_price(["Shore Crane"]).select(pl.col("Price")).to_series()[0]
)

TRANSFER_PRICE = get_price(["Haulage FEU", "Haulage TEU"]).drop("end")

# Shore crane overtime increase by 10%

INCREASE_10_PERCENT = 1.10
CUT_OFF_DATE = date(2026, 3, 1)

shore_crane: pl.LazyFrame = (
    load_gsheet_data(TRANSPORT_SHEET_ID, shore_crane_sheet)
    .and_then(lambda x: x.filter(pl.col("date").dt.year().eq(CURRENT_YEAR)))
    # .filter(pl.col("date").dt.year().eq(CURRENT_YEAR))
    .select(
        pl.col("day").cast(dtype=DayName.enum_dtype()),
        cs.contains("date"),
        pl.col("start_time"),
        pl.col("end_time"),
        pl.col("hours").dt.hour(),  # str.to_time(format="%H:%M:%S")
        pl.col("overtime_hours")
        # .str.to_time(format="%H:%M")
        .dt.hour(),  # .str.to_time(format="%H:%M")
        pl.col("customer").cast(pl.Utf8),
        pl.col("location").cast(pl.Utf8),
        pl.col("operation_type"),
        pl.col("remarks"),
        pl.col("invoiced_to"),
        # pl.col("price").cast(pl.Float64),
        # pl.col("total_price").str.replace_many(["$", ","], "").cast(pl.Float64),
    )
    .with_columns(
        pl.when(
            (pl.col("day").is_in(SPECIAL_DAYS)).and_(pl.col("date").ge(CUT_OFF_DATE))
        )
        .then(SHORE_CRANE_PRICE * OvertimePerc.overtime_150 * INCREASE_10_PERCENT)
        .when((pl.col("day").is_in(SPECIAL_DAYS)))
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
    .and_then(lambda x: x.filter(pl.col("date").dt.year().eq(CURRENT_YEAR)))
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
    .sort(["Service", "date"])
    .join_asof(
        TRANSFER_PRICE.lazy().sort(["Service", "date"]),
        by="Service",
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
        .then(FREE)
        .when(
            (pl.col("day_name").is_in(SPECIAL_DAYS))
            & (pl.col("time") > UPPER_BOUND_SPECIAL_DAY)
        )
        .then(SHIFTING_PRICE * OvertimePerc.overtime_200)
        .when((pl.col("day_name").is_in(SPECIAL_DAYS)) | (pl.col("time") > UPPER_BOUND))
        .then(SHIFTING_PRICE * OvertimePerc.overtime_150)
        .otherwise(SHIFTING_PRICE),
        haulage_price=pl.when((~pl.col("driver").cast(pl.Utf8).str.contains("IPHS")))
        .then(pl.lit(0))
        .when(
            (pl.col("day_name").is_in(SPECIAL_DAYS))
            & (pl.col("time") > UPPER_BOUND_SPECIAL_DAY)
        )
        .then(pl.col("Price") * OvertimePerc.overtime_200)
        .when(
            (
                (pl.col("day_name").is_in(SPECIAL_DAYS))
                | (pl.col("time") > UPPER_BOUND)
            ).or_(pl.col("time").is_between(MIDNIGHT, LOWER_BOUND))
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
    .and_then(lambda x: x.filter(pl.col("date").dt.year().eq(CURRENT_YEAR)))
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
    .and_then(lambda x: x.filter(pl.col("date").dt.year().eq(CURRENT_YEAR)))
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
