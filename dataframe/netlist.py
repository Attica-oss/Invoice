"""NetList Lazyframes.

Every frame is an ``@lru_cache`` builder exposed under its historical name
via :pep:`562` ``__getattr__`` -- importing this module does no I/O.
"""

from datetime import date
from functools import lru_cache
from typing import Any

import polars as pl

import dataframe.stuffing as _stuffing
from data.price import get_price
from data_source.all_dataframe import miscellaneous
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import (
    NET_LIST_SHEET_NAME,
    # MISC_SHEET_ID,
    OPS_SHEET_ID,
    # by_catch_sheet,
    # all_cccs_data_sheet,
    RAW_DATA_SHEET_NAME,
)
from type_casting.containers import iot_soc
from type_casting.customers import cargo
from type_casting.dates import (
    CURRENT_YEAR,
    SPECIAL_DAYS,
    public_holiday,
)
from type_casting.validations import (
    # FISH_STORAGE,
    # MOVEMENT_TYPE,
    UNLOADING_SERVICE,
    Overtime,
    OvertimePerc,
)

ph_list: pl.Series = public_holiday()


service_list: list[str] = [
    "Transhipment - Brine",
    "Transhipment - Dry",
    "Unload to CCCS - Brine",
    "Unload to CCCS - Dry",
    "Unload to Quay - Brine",
    "Unload to Quay - Dry",
    "Container Stuffing - Brine",
    "Container Stuffing - Dry",
    "Full OSS - Brine",
    "Full OSS - Dry",
    "Basic OSS - Brine",
    "Basic OSS - Dry",
]

oss_service_list: list[str] = [
    "Container Stuffing - Brine",
    "Container Stuffing - Dry",
    "Stuffing",
]


# Price
@lru_cache(maxsize=1)
def _prices() -> dict[str, pl.DataFrame]:
    """NetList prices from one cached read of the price table."""
    return {
        "stuffing": get_price(["Stuffing"]),
        "iot_cargo": get_price(["Loading to Cargo - Brine", "Loading to Cargo - Dry"]),
        "unloading": get_price(service_list).with_columns(date=pl.col("date")),
        "oss_stuffing": get_price(oss_service_list).with_columns(date=pl.col("date")),
    }


by_catch_companies = [
    "AMIRANTE",
    "OCEAN BASKET",
    "ISLAND CATCH",
]  # move this to the customer module


# Container Stuffing Type


@lru_cache(maxsize=1)
def _stuffing_type() -> pl.LazyFrame:
    return (
        # .filter(pl.col("customer") != "IOT")
        _stuffing.coa.select(
            pl.col(
                [
                    "vessel_client",
                    "customer",
                    "date_plugged",
                    "container_number",
                    "time_plugged",
                    "operation_type",
                ]
            )
        )
        .with_columns(
            pl.col("container_number").cast(pl.Utf8),
            pl.col("vessel_client").cast(pl.Utf8),
        )
        .filter(
            (~pl.col("operation_type").str.contains("CCCS"))
            & (pl.col("operation_type").str.contains_any(["Full", "Basic", "Stuffing"]))
            & (~pl.col("operation_type").str.contains("Cross"))
        )
        .with_columns(
            stuffing=pl.when(pl.col("operation_type").str.contains("Full"))
            .then(pl.lit("Full OSS"))
            .when(pl.col("operation_type").str.contains("Basic"))
            .then(pl.lit("Basic OSS"))  # Changed to Basic OSS
            .otherwise(pl.lit("Container Stuffing"))
        )
        .group_by(
            [
                "vessel_client",
                "customer",
                "date_plugged",
                "container_number",
                "stuffing",
            ]
        )
        .agg(pl.col("time_plugged").max())
        .drop("time_plugged")
        .select(pl.all().exclude("operation_type"))
        .unique(subset=["vessel_client", "container_number", "date_plugged"], keep="last")
        .sort(by="date_plugged")
    )


# CCCS record from the Miscellaneous Activity
@lru_cache(maxsize=1)
def _cccs_record() -> pl.LazyFrame:
    return (
        (
            miscellaneous()
            .filter(
                pl.col("operation_type").is_in(UNLOADING_SERVICE),
                ~pl.col("customer").is_in(by_catch_companies),
            )
            .with_columns(
                destination="CCCS ("
                + pl.col("customer")
                .str.replace(" S.A.", "")  # For CFTO
                .str.replace(" S.A", "")  # For INPESCA
                .str.replace(" SA", "")  # For ALBACORA
                .cast(pl.Utf8)
                + ")"
            )
            .select(
                pl.col("day_name"),
                pl.col("date"),
                pl.col("movement_type"),
                pl.col("destination"),
                pl.col("vessel"),
                pl.col("operation_type"),
                pl.col("total_tonnage"),
                pl.col("overtime_tonnage"),
                pl.col("storage_type"),
            )
        )
        .group_by(["date", "destination", "vessel", "storage_type"])
        .agg(pl.col("total_tonnage").sum(), pl.col("overtime_tonnage").sum())
        .sort(by="date")
    )


# CCCS adjusted record in the Genesis Data
@lru_cache(maxsize=1)
def _cccs_adjusted_records() -> pl.LazyFrame:
    return (
        (
            load_gsheet_data(sheet_id=OPS_SHEET_ID, sheet_name=RAW_DATA_SHEET_NAME)
            .filter(
                pl.col("Container (Destination)")
                .str.contains("CCCS")
                .and_(pl.col("Date").dt.year().eq(CURRENT_YEAR))
            )
            .select(
                pl.col("Date").days.add_day_name(),  # type: ignore[attr-defined]
                pl.col("Date").alias("date"),
                pl.col("Time"),
                pl.col("overtime"),
                pl.col("Storage").alias("storage_type"),
                pl.col("Vessel").str.to_uppercase().alias("vessel"),
                (
                    pl.col("Scale Reading(-Fish Net) (Cal)").str.replace(",", "").cast(pl.Int64)
                    * 0.001  # Convert to Tons from Kilos
                )
                .round(3)
                .cast(pl.Float64)
                .alias("total_tonnage"),
                pl.col("Container (Destination)").alias("destination"),
                pl.col("Species"),
            )
            .select(
                pl.all(),
                pl.col("total_tonnage")
                .sum()
                .over(["date", "vessel", "destination", "overtime", "storage_type"])
                .alias("tons"),
            )
            .with_columns(
                tonnage_select=pl.when(
                    (
                        (pl.col("day_name").is_in(SPECIAL_DAYS)).and_(
                            pl.col("overtime").eq(Overtime.overtime_150_text)
                        )
                    ).or_(pl.col("overtime") == Overtime.normal_hour_text)
                )
                .then(pl.lit("normal"))
                .when(
                    (pl.col("overtime") == Overtime.overtime_150_text)
                    | (pl.col("overtime") == Overtime.overtime_200_text)
                )
                .then(pl.lit("overtime"))
                .otherwise(pl.lit("ERR"))  # To modify this for the "Invalid invoice"
            )
        )
        .join(
            _cccs_record(),
            on=["date", "destination", "vessel", "storage_type"],
            how="left",
        )
        .with_columns(normal_tonnage=pl.col("total_tonnage_right") - pl.col("overtime_tonnage"))
        .with_columns(
            perc_diff=pl.when(pl.col("tonnage_select") == "normal")
            .then(pl.col("normal_tonnage") / pl.col("tons"))
            .otherwise(pl.col("overtime_tonnage") / pl.col("tons"))
        )
        .with_columns(adjusted_tonnage=pl.col("total_tonnage") * pl.col("perc_diff"))
        .group_by(["day_name", "date", "overtime", "vessel", "destination", "storage_type"])
        .agg(
            start_time=pl.col("Time").min(),
            end_time=pl.col("Time").max(),
            total_tonnage=pl.col("adjusted_tonnage").sum().round(3),
        )
        .select(
            [
                "day_name",
                "date",
                "vessel",
                "start_time",
                "destination",
                "overtime",
                "storage_type",
                "end_time",
                "total_tonnage",
            ]
        )
        .sort(by="date")
    )


# The Net List


@lru_cache(maxsize=1)
def _netList() -> pl.LazyFrame:
    stuffing_type = _stuffing_type()
    UNLOADING_PRICE = _prices()["unloading"]
    return (
        pl.concat(
            [
                load_gsheet_data(sheet_id=OPS_SHEET_ID, sheet_name=NET_LIST_SHEET_NAME)
                .filter(pl.col("Date").dt.year().eq(CURRENT_YEAR))
                .filter(~pl.col("Container (Destination)").str.contains("CCCS"))
                .select(
                    pl.col("Date").days.add_day_name(),  # type: ignore[attr-defined]
                    pl.col("Date").alias("date"),
                    pl.col("Vessel").str.to_uppercase().alias("vessel"),
                    pl.col("startTime").alias("start_time"),
                    pl.col("Container (Destination)").alias("destination"),
                    pl.col("overtime"),
                    pl.col("Storage").alias("storage_type"),
                    pl.col("endTime").alias("end_time"),
                    pl.col("Total Tonnage").round(3).alias("total_tonnage"),
                ),
                _cccs_adjusted_records(),
            ],
            how="vertical",
        )
        .sort(by="date")
        .join_asof(
            stuffing_type,
            left_on="date",
            right_on="date_plugged",
            by_left=["destination", "vessel"],
            by_right=["container_number", "vessel_client"],
            strategy="forward",
            tolerance="2d",
        )
        .join(
            other=stuffing_type,
            left_on=["destination", "date"],
            right_on=["container_number", "date_plugged"],
            how="left",
            suffix="_ox",
            coalesce=False,
        )
        .drop(["customer_ox", "date_plugged_ox"])
        .with_columns(
            service=pl.when(
                pl.col("stuffing_ox")
                .eq(pl.lit("Basic OSS"))
                .and_(
                    pl.col("vessel_client").is_in(["OCEAN BASKET", "AMIRANTE", "ISLAND CATCH"])
                )
            )
            .then(pl.col("stuffing_ox"))
            .when(
                (pl.col("destination").str.contains("IOT"))
                | (
                    (pl.col("destination").str.contains("DARDANEL")).and_(
                        pl.col("date").lt(
                            pl.lit(date(2025, 9, 1))
                        )  # Dardanel not getting discount after 1st September 2025
                    )
                )
                | (
                    pl.col("destination")
                    .str.contains("Asian Marine Reefer")
                    .and_(
                        pl.col("date").is_between(date(2025, 11, 3), date(2025, 11, 15))
                    )  # Asian Marine Reefer is for IOT
                )
                | (pl.col("destination").str.contains("Unload to Quay"))
                | (
                    pl.col("destination")
                    .is_in(iot_soc)
                    .and_(pl.col("customer").eq(pl.lit("IOT")))
                )
            )
            .then(pl.lit("Unload to Quay"))
            .when(pl.col("destination").str.to_uppercase().is_in(cargo))
            .then(pl.lit("Transhipment"))
            .when(pl.col("destination").str.contains("CCCS"))
            .then(pl.lit("Unload to CCCS"))
            .otherwise(pl.col("stuffing"))
        )
        .select(
            pl.all().exclude(
                [
                    # "operation_type",
                    "stuffing",
                    "customer",
                    "customer_ox",
                    # "operation_type_ox",
                    "stuffing_ox",
                    "date_plugged",
                    "date_plugged_ox",
                    "container_number",
                ]
            )
        )
        .with_columns(service=pl.col("service") + " - " + pl.col("storage_type"))
        .sort(by="date")
        .join_asof(
            UNLOADING_PRICE.lazy(),
            by_left="service",
            by_right="service",
            on="date",
            strategy="backward",
        )
        .drop("service_type")
        .with_columns(
            unit_price=pl.when(pl.col("overtime") == Overtime.overtime_200_text)
            .then(pl.col("unit_price") * OvertimePerc.overtime_200)
            .when(pl.col("overtime") == Overtime.overtime_150_text)
            .then(pl.col("unit_price") * OvertimePerc.overtime_150)
            .otherwise(pl.col("unit_price"))
        )
        .with_columns(
            pl.when(
                pl.col("vessel_client").is_in(by_catch_companies)
                # & pl.col("vessel_client").ne(pl.col("vessel"))
            )
            .then(pl.col("vessel_client"))
            .otherwise(pl.lit(None))
            .alias("remarks")
        )
        .drop("vessel_client")
        .with_columns(
            invoice_value=(
                pl.col("unit_price").round(3) * pl.col("total_tonnage").round(3)
            ).cast(pl.Decimal(scale=3))
        )
        .select(
            pl.col("day_name"),
            pl.col("date"),
            pl.col("vessel"),
            pl.col("start_time"),
            pl.col("destination"),
            pl.col("overtime"),
            pl.col("storage_type"),
            pl.col("end_time"),
            pl.col("total_tonnage"),
            pl.col("service"),
            pl.col("unit_price"),
            pl.col("invoice_value"),
            pl.col("remarks"),
        )
        .unique()
    )


# Maersk OSS stuffing list ; Separated between Full and Basic OSS
@lru_cache(maxsize=1)
def _oss() -> pl.LazyFrame:
    OSS_STUFFING_PRICE = _prices()["oss_stuffing"]
    return (
        _netList()
        .select(pl.all().exclude(["unit_price", "invoice_value"]))
        .filter(pl.col("service").str.contains("OSS"))
        .with_columns(
            vessel=pl.when(pl.col("remarks").is_null())
            .then(pl.col("vessel"))
            .otherwise(pl.col("remarks"))
        )
        .with_columns(
            service=pl.when(pl.col("service") == pl.lit("Full OSS"))
            .then(pl.lit("Container Stuffing") + " - " + pl.col("storage_type"))
            .otherwise(pl.lit("Stuffing"))
        )
        .join_asof(OSS_STUFFING_PRICE.lazy(), by="service", on="date", strategy="backward")
        .select(pl.all().exclude(["service"]))
        .with_columns(
            Price=pl.when(pl.col("overtime") == Overtime.overtime_200_text)
            .then(pl.col("unit_price") * OvertimePerc.overtime_200)
            .when(pl.col("overtime") == Overtime.overtime_150_text)
            .then(pl.col("unit_price") * OvertimePerc.overtime_150)
            .otherwise(pl.col("unit_price"))
        )
        .with_columns(invoice_value=(pl.col("unit_price") * pl.col("total_tonnage")).round(3))
        .select(pl.all().exclude("remarks"))
    )


# IOT SOC Stuffing
get_iot_containers: pl.Expr = pl.col("container_number").is_in(iot_soc)

get_iot_cargo: pl.Expr = (
    pl.col("destination")
    .is_in(["Asian Marine Reefer"])
    .and_(pl.col("date").ge(pl.lit(date(2025, 11, 1))))
)


# Create an IOT list of containers stuffed on IOT account.
@lru_cache(maxsize=1)
def _iot_coa() -> pl.LazyFrame:
    return (
        _stuffing.coa.with_columns(
            pl.col("vessel_client").cast(pl.Utf8),
            pl.col("container_number").cast(pl.Utf8),
        )
        .select(
            [
                "vessel_client",
                "customer",
                "operation_type",
                "shipping_line",
                "date_plugged",
                "container_number",
            ]
        )
        .filter(
            pl.col("shipping_line").eq(pl.lit("IOT")),
            pl.col("operation_type").str.contains(pl.lit("Stuffing")),
        )
        .select(pl.col("*").exclude(["operation_type", "shipping_line"]))
    )


@lru_cache(maxsize=1)
def _iot_cargo() -> pl.LazyFrame:
    IOT_CARGO_PRICE = _prices()["iot_cargo"]
    return (
        (
            load_gsheet_data(OPS_SHEET_ID, NET_LIST_SHEET_NAME)
            .select(
                pl.col("Date").alias("date"),
                pl.col("Vessel").str.to_uppercase().alias("vessel"),
                pl.col("startTime").alias("start_time"),
                pl.col("Container (Destination)").alias("destination"),
                pl.col("overtime"),
                pl.col("Storage").alias("storage"),
                pl.col("endTime").alias("end_time"),
                pl.col("Total Tonnage").alias("total_tonnage"),
            )
            .filter(get_iot_cargo)
        )
        .with_columns(
            day_name=pl.when(pl.col("date").is_in(ph_list))
            .then(pl.lit("PH"))
            .otherwise(pl.col("date").dt.to_string(format="%a")),
            service=pl.lit("Loading to Cargo") + " - " + pl.col("storage"),
        )
        .join_asof(
            IOT_CARGO_PRICE.lazy(),
            by="service",
            left_on="date",
            right_on="date",
            strategy="backward",
        )
        .drop("service")
        .with_columns(
            total_price=pl.when(pl.col("overtime") == "normal hours")
            .then(pl.col("total_tonnage") * pl.col("unit_price") * OvertimePerc.normal_hour)
            .when(pl.col("overtime") == "overtime 150%")
            .then(pl.col("total_tonnage") * pl.col("unit_price") * OvertimePerc.overtime_150)
            .when(pl.col("overtime") == "overtime 200%")
            .then(pl.col("total_tonnage") * pl.col("unit_price") * OvertimePerc.overtime_200)
            .otherwise(0)
        )
        .select(
            pl.col("vessel"),
            pl.col("day_name"),
            pl.col("date"),
            pl.col("start_time"),
            pl.col("destination"),
            pl.col("end_time"),
            pl.col("overtime"),
            pl.col("total_tonnage"),
            pl.col("storage"),
            pl.col("unit_price").alias("unit_price"),
            pl.col("total_price").round(3),
        )
    )


# IOT SOC Stuffing DataFrame
@lru_cache(maxsize=1)
def _iot_stuffing() -> pl.LazyFrame:
    STUFFING_PRICE = _prices()["stuffing"]
    return (
        load_gsheet_data(OPS_SHEET_ID, NET_LIST_SHEET_NAME)
        .filter(pl.col("Date").dt.year().eq(CURRENT_YEAR))
        .select(
            pl.col("Date").alias("date"),
            pl.col("Vessel").str.to_uppercase().alias("vessel"),
            pl.col("startTime").alias("start_time"),
            pl.col("Container (Destination)").alias("container_number"),
            pl.col("overtime"),
            pl.col("Storage").alias("storage"),
            pl.col("endTime").alias("end_time"),
            pl.col("Total Tonnage").alias("total_tonnage"),
        )
        .filter(get_iot_containers)
        .with_columns(
            day_name=pl.when(pl.col("date").is_in(ph_list))
            .then(pl.lit("PH"))
            .otherwise(pl.col("date").dt.to_string(format="%a")),
            service=pl.lit("Stuffing"),
        )
        .join_asof(
            STUFFING_PRICE.lazy(),
            by=None,
            left_on="date",
            right_on="date",
            strategy="backward",
        )
        .select(pl.all().exclude(["service"]))
        .with_columns(
            total_price=pl.when(pl.col("overtime") == "normal hours")
            .then(pl.col("total_tonnage") * pl.col("unit_price") * OvertimePerc.normal_hour)
            .when(pl.col("overtime") == "overtime 150%")
            .then(pl.col("total_tonnage") * pl.col("unit_price") * OvertimePerc.overtime_150)
            .when(pl.col("overtime") == "overtime 200%")
            .then(pl.col("total_tonnage") * pl.col("unit_price") * OvertimePerc.overtime_200)
            .otherwise(0)
        )
        .select(
            pl.col(
                [
                    "vessel",
                    "day_name",
                    "date",
                    "start_time",
                    "container_number",
                    "end_time",
                    "overtime",
                    "total_tonnage",
                    "storage",
                    "unit_price",
                    "total_price",
                ]
            )
        )
        .join(
            _iot_coa(),
            left_on=["date", "vessel", "container_number"],
            right_on=["date_plugged", "vessel_client", "container_number"],
            how="left",
        )
        .filter(pl.col("customer").eq(pl.lit("IOT")))
        .select(pl.col("*").exclude(["customer"]))
    )


_EXPORTS = {
    "stuffing_type": _stuffing_type,
    "cccs_record": _cccs_record,
    "cccs_adjusted_records": _cccs_adjusted_records,
    "netList": _netList,
    "oss": _oss,
    "iot_coa": _iot_coa,
    "iot_cargo": _iot_cargo,
    "iot_stuffing": _iot_stuffing,
}


def __getattr__(name: str) -> Any:
    builder = _EXPORTS.get(name)
    if builder is not None:
        return builder()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = list(_EXPORTS)
