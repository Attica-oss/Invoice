"""Washing Dataset"""

import polars as pl

from data import ServiceType, get_price_by_type
from data_source import EMR_SHEET_ID, PTI_SHEET_NAME, load_gsheet_data

# from dataframe.emr import WASHING
from type_casting import CURRENT_YEAR, SetPoint, containers_enum

pre_trip_inspection_price: pl.LazyFrame = get_price_by_type(
    service_type=ServiceType.pre_trip_inspection
).drop(["service_type"])

S_FREEZER_PTI_ELECTRICITY = (
    pre_trip_inspection_price.select(pl.col("unit_price"))
    .filter(pl.col("service").eq("PTI S Freezer"))
    .collect()
    .to_series()
)

MAGNUM_PTI_ELECTRICITY = (
    pre_trip_inspection_price.select(pl.col("unit_price"))
    .filter(pl.col("service").eq("PTI Magnum"))
    .collect()
    .to_series()
)

STANDARD_PTI_ELECTRICITY = (
    pre_trip_inspection_price.select(pl.col("unit_price"))
    .filter(pl.col("service").eq("PTI Standard"))
    .collect()
    .to_series()
)


PLUGIN_PRICE: pl.Series = (
    get_price_by_type(service_type=ServiceType.electricity)
    .filter(pl.col("service").eq("Plugin"))
    .collect()
    .to_series()
)


def _electricity_price_expr() -> pl.Expr:
    """
    Per-session electricity price, before the >8h multiplier.
    IOT is billed per day-elapsed (+1 base day); everyone else is a flat
    rate keyed off the container's set point.
    """
    days_elapsed = _hours_between("datetime_start", "datetime_end") / 24
    return (
        pl.when(pl.col("invoice_to") == pl.lit("IOT"))
        .then(days_elapsed + 1)
        .when(pl.col("set_point") == SetPoint.s_freezer)
        .then(S_FREEZER_PTI_ELECTRICITY)
        .when(pl.col("set_point") == SetPoint.magnum)
        .then(MAGNUM_PTI_ELECTRICITY)
        .when(pl.col("set_point") == SetPoint.standard)
        .then(STANDARD_PTI_ELECTRICITY)
        .otherwise(0)
    )


# ── Helper expressions ────────────────────────────────────────────────
def _hours_between(start_col: str, end_col: str) -> pl.Expr:
    """Elapsed time in hours, rounded to 2 dp, computed consistently everywhere."""
    return ((pl.col(end_col) - pl.col(start_col)).dt.total_minutes() / 60).round(2)


PTI_OUTPUT_COLUMNS = [
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


# ── PTI staging data set ──────────────────────────────────────────────
pti_staging: pl.LazyFrame = (
    load_gsheet_data(EMR_SHEET_ID, PTI_SHEET_NAME)
    .filter(pl.col("datetime_start").dt.year().eq(CURRENT_YEAR))
    .select(
        pl.col("datetime_start"),
        pl.col("container_number").cast(containers_enum),
        pl.col("set_point").cast(pl.Utf8).cast(SetPoint.enum_dtype()),
        pl.col("unit_manufacturer"),
        pl.col("datetime_end"),
        pl.col("status").cast(pl.Enum(["PASSED", "FAILED"])),
        pl.col("invoice_to").cast(pl.Enum(["MAERSKLINE", "IOT", "INVALID", "CMA CGM"])),
        pl.col("plugged_on").alias("generator"),
    )
    .with_columns(
        _hours_between("datetime_start", "datetime_end").alias("hours"),
        pl.lit(PLUGIN_PRICE).alias("plugin_price"),
    )
    .with_columns(
        # Sessions over 8h get double the base electricity rate.
        above_8_hours=pl.when(pl.col("hours") > 8).then(2).otherwise(1)
    )
    .with_columns(electricity_price=_electricity_price_expr() * pl.col("above_8_hours"))
    .with_columns(
        # 1-indexed sequence of each container's PTI sessions, in source order.
        cum_count=pl.col("container_number").cum_count().over("container_number")
    )
)


# Washing Data Set
def pre_trip_inspection() -> pl.LazyFrame:
    return (
        load_gsheet_data(sheet_id=EMR_SHEET_ID, sheet_name=PTI_SHEET_NAME)
        .filter(
            pl.col("date")
            .dt.year()
            .eq(CURRENT_YEAR)
            .and_(pl.col("invoice_to").ne("INVALID"))
        )
        .select(
            pl.col("date"),
            pl.col("container_number").cast(dtype=containers_enum),
            pl.col("invoice_to"),
            pl.col("service_remarks"),
        )
        .sort(by="date")
        .join_asof(
            cleaning_price,
            by=None,
            left_on="date",
            right_on="date",
            strategy="backward",
        )
    )
