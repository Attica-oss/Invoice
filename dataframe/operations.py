"""Operations Lazyframe for parquet storage only"""

# from pathlib import Path
import polars as pl
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import OPS_SHEET_ID, raw_sheet
from type_casting.validations import FISH_STORAGE
from type_casting.validations import OvertimePerc

from type_casting.dates import SPECIAL_DAYS, public_holiday, DAY_NAMES

# from dataframe import invoice
from data_source.excel_file_path import ExcelFiles
from data.price import get_price


# To move the Price to the Price module

EXTRAMEN: pl.DataFrame = get_price(["Extra Men"]).with_columns(date=pl.col("end"))

WELL_TO_WELL: pl.DataFrame = get_price(["Well to Well Transfer"]).with_columns(
    date=pl.col("end")
)

TARE_RATE: pl.DataFrame = (
    get_price(["Rental of Calibration", "Tare Calibration"])
    .with_columns(date=pl.col("end"))
    .select(
        pl.col("date").alias("effective_date"),
        pl.col("Service").alias("service"),
        pl.col("Price").alias("unit_price"),
    )
)


ADDITIONAL_OVERTIME: pl.DataFrame = (
    get_price(["Additional Overtime"]).with_columns(date=pl.col("end")).drop("end")
)

# Path to the operations activity file
OPS_ACTIVITY_PATH = ExcelFiles.OPERATIONS_ACTIVITY_2026.value


# Operations Activity Unloading Lazyframe
ops: pl.LazyFrame = (
    load_gsheet_data(sheet_id=OPS_SHEET_ID, sheet_name=raw_sheet)
    .unwrap()
    .select(
        pl.col("Day"),
        pl.col("Date"),
        pl.col("Time"),
        pl.col("Vessel").str.to_uppercase(),
        pl.col("Species").str.extract(r"^(.*?)(\s-\s)"),
        pl.col("Details").str.to_uppercase(),
        pl.col("Scale Reading(-Fish Net) (Cal)")
        .str.replace(",", "")
        .cast(pl.Int64)
        .alias("tonnage")
        * 0.001,
        pl.col("Storage").cast(dtype=pl.Enum(FISH_STORAGE)),
        pl.col("Container (Destination)").alias("destination"),
        pl.col("overtime"),
        pl.col("Side Working"),
    )
)


def add_day_name_column(date_col: pl.Expr) -> pl.Expr:
    """adds the day name based on the date column name this includes public holiday (PH)"""

    return (
        pl.when(date_col.is_in(public_holiday()))
        .then(pl.lit("PH"))
        .otherwise(date_col.dt.to_string(format="%a"))
    ).cast(dtype=pl.Enum(DAY_NAMES))


main_file: pl.LazyFrame = (
    pl.read_excel(
        OPS_ACTIVITY_PATH[0], sheet_name=OPS_ACTIVITY_PATH[1], engine="calamine"
    )
    .filter(pl.col("DAY") != "", pl.col("DAY") != "Total")
    .lazy()
)

handling_activity = main_file.select(
    pl.col("DATE").alias("date"),
    pl.col("VESSEL NAME").str.to_uppercase().alias("vessel_name"),
    pl.col("OPERATION TYPE"),
    pl.col("BRINE (SAUMURE)"),
    pl.col("DRY (Below -30°C)"),
    pl.col("TOTAL TONNAGE"),
    pl.col("Well-to-Well Transfer").fill_null(0),
    pl.col("Overtime Tonnage"),
    pl.col("Extra Men").fill_null(0).cast(pl.Int32).alias("extra_men"),
    pl.col("Number of Stevedores").fill_null(0).cast(pl.Int32),
    pl.col("OPEX"),
    pl.col("OPEX %"),
    pl.col("Comments").alias("remarks"),
).with_columns(day_name=add_day_name_column(pl.col("date")))


# Extra men service

# Spliting EXpr

split_remarks = pl.col("remarks").str.splitn("/ ", n=2).struct.field("field_1")
split_again = (
    pl.col("remarks")
    .str.splitn("/ ", n=2)
    .struct.field("field_1")
    .str.splitn(" / ", n=2)
    .struct.field("field_1")
    .str.strip_chars()
)

extramen: pl.DataFrame = (
    handling_activity.select(
        pl.col("day_name"),
        pl.col("date"),
        pl.col("vessel_name").alias("vessel"),
        pl.col("TOTAL TONNAGE").alias("total_tonnage"),
        pl.col("extra_men"),
        pl.col("Number of Stevedores"),
        pl.col("remarks"),
    )
    .with_columns(check=(pl.col("Number of Stevedores") - 47).eq(pl.col("extra_men")))
    .with_columns(Service=pl.lit("Extra Men"))
    .filter(pl.col("extra_men") > 0)
    .sort(by="date")
    .join_asof(EXTRAMEN, by="Service", on="date", strategy="backward")
    .with_columns(
        total_price=pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
        .then(
            OvertimePerc.overtime_150
            * pl.col("Price")
            * pl.col("total_tonnage")
            * pl.col("extra_men")
        )
        .otherwise(
            OvertimePerc.normal_hour
            * pl.col("Price")
            * pl.col("total_tonnage")
            * pl.col("extra_men")
        )
    )
    .with_columns(
        price=pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
        .then(OvertimePerc.overtime_150 * pl.col("Price"))
        .otherwise(OvertimePerc.normal_hour * pl.col("Price"))
    )
    .with_columns(
        remarks=pl.when(split_remarks.str.starts_with("OT"))
        .then(split_again)
        .otherwise(split_remarks.str.strip_chars())
    )
    .select(
        pl.col("day_name"),
        pl.col("date"),
        pl.col("vessel"),
        pl.col("total_tonnage").round(3),
        (
            pl.when(pl.col("check"))
            .then(pl.col("extra_men"))
            .otherwise(pl.lit("check"))
        ).alias("extra_men"),
        pl.col("price").round(2),
        pl.col("total_price").round(3),
        pl.col("remarks"),
    )
    .sort(by="date")
)


# Well to well transfer

hatch_to_hatch: pl.LazyFrame = (
    handling_activity.filter(pl.col("Well-to-Well Transfer") > 0)
    .select(
        pl.col("day_name"),
        pl.col("date"),
        pl.col("vessel_name").alias("vessel"),
        pl.col("Well-to-Well Transfer"),
    )
    .with_columns(Service=pl.lit("Well to Well Transfer"))
    .join_asof(WELL_TO_WELL.lazy(), by="Service", on="date", strategy="backward")
    .with_columns(
        total_price=pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
        .then(
            OvertimePerc.overtime_150
            * pl.col("Price")
            * pl.col("Well-to-Well Transfer")
        )
        .otherwise(
            OvertimePerc.normal_hour * pl.col("Price") * pl.col("Well-to-Well Transfer")
        )
    )
    .with_columns(
        price=pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
        .then(OvertimePerc.overtime_150 * pl.col("Price"))
        .otherwise(OvertimePerc.normal_hour * pl.col("Price"))
    )
    .select(
        pl.col("day_name"),
        pl.col("date"),
        pl.col("vessel"),
        pl.col("Well-to-Well Transfer").alias("tonnage"),
        pl.col("price"),
        pl.col("total_price"),
    )
)

# Rental of Calibration Weight Service

tare: pl.LazyFrame = (
    (
        (
            (
                load_gsheet_data(OPS_SHEET_ID, raw_sheet).unwrap()
                .select(
                    pl.col("Date").alias("date"),
                    pl.col("Vessel").str.to_uppercase().alias("vessel"),
                    pl.col("Side Working").alias("side_working"),
                )
                .unique()
                .sort(by="date")
                .group_by(["date", "vessel"], maintain_order=True)
                .agg(
                    pl.col("side_working")
                    .unique()
                    .sort()
                    .str.join(", ")
                    .alias("side_working")
                )
            )
            .with_columns(
                [
                    pl.lit(1, dtype=pl.Int64).alias("rental_of_weight"),
                    pl.col("side_working")
                    .str.split(", ")
                    .list.len()
                    .alias("number_of_sides"),
                    pl.lit("Rental of Calibration").alias("service"),
                ]
            )
            .join_asof(
                other=TARE_RATE.lazy(),
                by_left="service",
                by_right="service",
                left_on="date",
                right_on="effective_date",
                strategy="backward",
            )
            .drop(["service", "effective_date"])
        )
        .with_columns(
            [
                (pl.col("unit_price") * pl.col("rental_of_weight")).alias(
                    "price_per_rental"
                )
            ]
        )
        .drop(pl.col("unit_price"))
    )
    .with_columns(pl.lit("Tare Calibration").alias("service"))
    .join_asof(
        other=TARE_RATE.lazy(),
        by_left="service",
        by_right="service",
        left_on="date",
        right_on="effective_date",
        strategy="backward",
    )
    .drop(["service", "effective_date"])
    .with_columns(
        [
            (pl.col("unit_price") * pl.col("number_of_sides")).alias(
                "price_per_calibrations"
            )
        ]
    )
    .drop(pl.col("unit_price"))
    .with_columns(
        [
            (pl.col("price_per_rental") + pl.col("price_per_calibrations")).alias(
                "total_price"
            )
        ]
    )
)


# Additional Stevedores (Overtime)

# additional: pl.LazyFrame = (
#     invoice.additional_overtime.select(
#         pl.col("date"),
#         pl.col("vessel").str.to_uppercase(),
#         pl.col("overtime_tonnage"),
#         pl.col("start_time"),
#         pl.col("end_time"),
#         pl.col("number_of_stevedores").str.replace("", "0").cast(pl.Int32),
#     )
#     .with_columns(
#         hours=(
#             (
#                 pl.col("date").dt.combine(pl.col("end_time"))
#                 - pl.col("date").dt.combine(pl.col("start_time"))
#             ).dt.total_minutes()
#             / 60
#         )
#         .ceil()
#         .cast(pl.Int32),
#         Service=pl.lit("Additional Overtime"),
#     )
#     .sort(by="date")
#     .join_asof(ADDITIONAL_OVERTIME, by="Service", on="date", strategy="backward")
#     .with_columns(
#         total_price=pl.col("Price") * pl.col("hours") * pl.col("number_of_stevedores")
#     )
#     .with_columns(date_and_time=pl.col("date").dt.combine(pl.col("end_time")))
#     .select(
#         pl.col("date_and_time"),
#         pl.col("vessel"),
#         pl.col("number_of_stevedores"),
#         pl.col("hours").alias("number_of_hours"),
#         pl.col("overtime_tonnage"),
#         pl.col("Price"),
#         pl.col("total_price"),
#     )
# )
