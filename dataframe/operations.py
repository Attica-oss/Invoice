"""Operations Lazyframe for parquet storage only.

Frames and prices are built lazily by ``@lru_cache`` functions and exposed
under their historical module-level names via :pep:`562` ``__getattr__`` --
importing this module reads no Excel files and does no network I/O.
"""

from functools import lru_cache
from typing import Any

import polars as pl

from data.price import get_price
from data_source.excel_file_path import ExcelFiles
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import OPS_SHEET_ID, RAW_DATA_SHEET_NAME
from type_casting.dates import CURRENT_YEAR, SPECIAL_DAYS, DayName, public_holiday
from type_casting.validations import FISH_STORAGE, OvertimePerc

# Path to the operations activity file
OPS_ACTIVITY_PATH = ExcelFiles.OPERATIONS_ACTIVITY_2026.value
ADDITIONAL_OVERTIME_PATH = ExcelFiles.ADDITIONAL_OVERTIME.value
BERTH_DUES_PATH = ExcelFiles.BERTH_DUES_2026.value


<<<<<<< HEAD
berth: pl.LazyFrame = pl.read_excel(
    BERTH_DUES_PATH[0],
    sheet_name=BERTH_DUES_PATH[1],
    engine="calamine",
    schema_overrides={"TIME IN": pl.Time, "TIME OUT": pl.Time},
).lazy()
=======
# ---------------------------------------------------------------------------
# Prices  (TODO: move to the price module)
# ---------------------------------------------------------------------------
@lru_cache(maxsize=1)
def _extramen_price() -> pl.DataFrame:
    return get_price(["Extra Men"]).with_columns(date=pl.col("date"))
>>>>>>> 0434db2afac254d931c9d2efacd59961dd278a45


@lru_cache(maxsize=1)
def _well_to_well_price() -> pl.DataFrame:
    return get_price(["Well to Well Transfer"]).with_columns(date=pl.col("date"))


@lru_cache(maxsize=1)
def _tare_rate() -> pl.DataFrame:
    return (
        get_price(["Rental of Calibration", "Tare Calibration"])
        .with_columns(date=pl.col("date"))
        .select(
            pl.col("date").alias("effective_date"),
            pl.col("service").alias("service"),
            pl.col("unit_price").alias("unit_price"),
        )
    )


@lru_cache(maxsize=1)
def _additional_overtime_price() -> pl.DataFrame:
    return get_price(["Additional Overtime"]).with_columns(date=pl.col("date"))


# ---------------------------------------------------------------------------
# Source frames
# ---------------------------------------------------------------------------
@lru_cache(maxsize=1)
def _berth() -> pl.LazyFrame:
    return pl.read_excel(
        BERTH_DUES_PATH[0],
        sheet_name=BERTH_DUES_PATH[1],
        engine="calamine",
        schema_overrides={"TIME IN": pl.Time, "TIME OUT": pl.Time},
    ).lazy()


@lru_cache(maxsize=1)
def _ops() -> pl.LazyFrame:
    return (
        load_gsheet_data(sheet_id=OPS_SHEET_ID, sheet_name=RAW_DATA_SHEET_NAME)
        .filter(pl.col("Date").dt.year().eq(CURRENT_YEAR))
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
        pl.when(date_col.is_in(public_holiday().implode()))
        .then(pl.lit("PH"))
        .otherwise(date_col.dt.to_string(format="%a"))
    ).cast(dtype=pl.Enum(DayName.list_all()))


@lru_cache(maxsize=1)
def _main_file() -> pl.LazyFrame:
    return (
        pl.read_excel(
            OPS_ACTIVITY_PATH[0], sheet_name=OPS_ACTIVITY_PATH[1], engine="calamine"
        )
        .filter(pl.col("DAY") != "", pl.col("DAY") != "Total")
        .lazy()
    )


@lru_cache(maxsize=1)
def _handling_activity() -> pl.LazyFrame:
    return (
        _main_file()
        .select(
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
        )
        .with_columns(day_name=add_day_name_column(pl.col("date")))
    )


# Extra men service -- remark-splitting expressions
split_remarks = pl.col("remarks").str.splitn("/ ", n=2).struct.field("field_1")
split_again = (
    pl.col("remarks")
    .str.splitn("/ ", n=2)
    .struct.field("field_1")
    .str.splitn(" / ", n=2)
    .struct.field("field_1")
    .str.strip_chars()
)


@lru_cache(maxsize=1)
def _extramen() -> pl.LazyFrame:
    return (
        _handling_activity()
        .select(
            pl.col("day_name"),
            pl.col("date"),
            pl.col("vessel_name").alias("vessel"),
            pl.col("TOTAL TONNAGE").alias("total_tonnage"),
            pl.col("extra_men"),
            pl.col("Number of Stevedores"),
            pl.col("remarks"),
        )
        .with_columns(
            check=(pl.col("Number of Stevedores") - 47).eq(pl.col("extra_men"))
        )
        .with_columns(service=pl.lit("Extra Men"))
        .filter(pl.col("extra_men") > 0)
        .sort(by="date")
        .join_asof(
            _extramen_price().lazy(), by="service", on="date", strategy="backward"
        )
        .with_columns(
            total_price=pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
            .then(
                OvertimePerc.overtime_150
                * pl.col("unit_price")
                * pl.col("total_tonnage")
                * pl.col("extra_men")
            )
            .otherwise(
                OvertimePerc.normal_hour
                * pl.col("unit_price")
                * pl.col("total_tonnage")
                * pl.col("extra_men")
            )
        )
        .with_columns(
            price=pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
            .then(OvertimePerc.overtime_150 * pl.col("unit_price"))
            .otherwise(OvertimePerc.normal_hour * pl.col("unit_price"))
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


@lru_cache(maxsize=1)
def _hatch_to_hatch() -> pl.LazyFrame:
    return (
        _handling_activity()
        .filter(pl.col("Well-to-Well Transfer") > 0)
        .select(
            pl.col("day_name"),
            pl.col("date"),
            pl.col("vessel_name").alias("vessel"),
            pl.col("Well-to-Well Transfer"),
        )
        .with_columns(service=pl.lit("Well to Well Transfer"))
        .join_asof(
            _well_to_well_price().lazy(), by="service", on="date", strategy="backward"
        )
        .with_columns(
            total_price=pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
            .then(
                OvertimePerc.overtime_150
                * pl.col("unit_price")
                * pl.col("Well-to-Well Transfer")
            )
            .otherwise(
                OvertimePerc.normal_hour
                * pl.col("unit_price")
                * pl.col("Well-to-Well Transfer")
            )
        )
        .with_columns(
            price=pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
            .then(OvertimePerc.overtime_150 * pl.col("unit_price"))
            .otherwise(OvertimePerc.normal_hour * pl.col("unit_price"))
        )
        .select(
            pl.col("day_name"),
            pl.col("date"),
            pl.col("vessel"),
            pl.col("Well-to-Well Transfer").alias("tonnage"),
            pl.col("unit_price"),
            pl.col("total_price"),
        )
    )


<<<<<<< HEAD
# Well to well transfer

hatch_to_hatch: pl.LazyFrame = (
    handling_activity.filter(pl.col("Well-to-Well Transfer") > 0)
    .select(
        pl.col("day_name"),
        pl.col("date"),
        pl.col("vessel_name").alias("vessel"),
        pl.col("Well-to-Well Transfer"),
    )
    .with_columns(service=pl.lit("Well to Well Transfer"))
    .join_asof(WELL_TO_WELL.lazy(), by="service", on="date", strategy="backward")
    .with_columns(
        total_price=pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
            .then(
                OvertimePerc.overtime_150
                * pl.col("unit_price")
                * pl.col("Well-to-Well Transfer")
            )
            .otherwise(
                OvertimePerc.normal_hour
                * pl.col("unit_price")
                * pl.col("Well-to-Well Transfer")
            )
    )
    .with_columns(
        price=pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
        .then(OvertimePerc.overtime_150 * pl.col("unit_price"))
        .otherwise(OvertimePerc.normal_hour * pl.col("unit_price"))
    )
    .select(
        pl.col("day_name"),
        pl.col("date"),
        pl.col("vessel"),
        pl.col("Well-to-Well Transfer").alias("tonnage"),
        pl.col("unit_price"),
        pl.col("total_price"),
    )
)

# Rental of Calibration Weight Service

tare: pl.LazyFrame = (
    (
=======
@lru_cache(maxsize=1)
def _tare() -> pl.LazyFrame:
    tare_rate = _tare_rate()
    return (
>>>>>>> 0434db2afac254d931c9d2efacd59961dd278a45
        (
            (
                (
                    _ops()
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
                    other=tare_rate.lazy(),
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
            .drop("unit_price")
        )
        .with_columns(pl.lit("Tare Calibration").alias("service"))
        .join_asof(
            other=tare_rate.lazy(),
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
        .drop("unit_price")
        .with_columns(
            [
                (pl.col("price_per_rental") + pl.col("price_per_calibrations")).alias(
                    "total_price"
                )
            ]
        )
    )


@lru_cache(maxsize=1)
def _additional() -> pl.LazyFrame:
    return (
        pl.read_excel(
            ADDITIONAL_OVERTIME_PATH[0],
            sheet_name=ADDITIONAL_OVERTIME_PATH[1],
            engine="calamine",
        )
        .lazy()
        .select(
            pl.col("Day"),
            pl.col("Date").alias("date"),
            pl.col("Vessel").alias("vessel"),
            pl.col("Tonnage").alias("overtime_tonnage"),
            pl.col("Hours").alias("hours"),
            pl.col("End Time").alias("end_time"),
            pl.col("Num of Stevedores")
            .str.replace("To check", "0")
            .cast(pl.Int32)
            .alias("number_of_stevedores"),
        )
        .with_columns(
            service=pl.lit("Additional Overtime"),
        )
        .sort(by="date")
        .join_asof(
            _additional_overtime_price().lazy(),
            by="service",
            on="date",
            strategy="backward",
        )
        .with_columns(
            total_price=pl.col("unit_price")
            * pl.col("hours")
            * pl.col("number_of_stevedores")
        )
        .select(
            pl.col("date"),
            pl.col("vessel"),
            pl.col("number_of_stevedores"),
            pl.col("hours").alias("number_of_hours"),
            pl.col("overtime_tonnage"),
            pl.col("unit_price"),
            pl.col("total_price"),
        )
        .filter(pl.col("number_of_stevedores") > 0)
    )


_EXPORTS = {
    "berth": _berth,
    "ops": _ops,
    "main_file": _main_file,
    "handling_activity": _handling_activity,
    "extramen": _extramen,
    "hatch_to_hatch": _hatch_to_hatch,
    "tare": _tare,
    "additional": _additional,
}


def __getattr__(name: str) -> Any:
    builder = _EXPORTS.get(name)
    if builder is not None:
        return builder()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = list(_EXPORTS)
