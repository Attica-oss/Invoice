import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")

with app.setup:
    from datetime import date,time
    from type_casting.dates import Days,SPECIAL_DAYS,UPPER_BOUND,UPPER_BOUND_SPECIAL_DAY
    import marimo as mo
    from pydantic import (
        BaseModel,
        Field,
        ValidationError,
        ConfigDict,
        field_validator,
        model_validator,
    )
    from enum import StrEnum
    import polars as pl
    from typing import Literal


@app.cell
def _():
    from data_source.make_dataset import load_gsheet_data

    return (load_gsheet_data,)


@app.cell
def _():
    from dataframe.bin_dispatch import full_scows

    return (full_scows,)


@app.class_definition
class DayName(StrEnum):
    """
    Enum representing day names including PH (Public Holiday).

    This enum provides a standardized way to represent days of the week
    and public holidays in the system.
    """

    PH = "PH"
    SUN = "Sun"
    MON = "Mon"
    TUE = "Tue"
    WED = "Wed"
    THU = "Thu"
    FRI = "Fri"
    SAT = "Sat"


@app.cell
def _(load_gsheet_data):
    customers_df = load_gsheet_data(sheet_id="1ai-zQMtbPUx0LeQeLmXcpPgKvL5cyvwDfSJqRzxfUQg",sheet_name="Client").unwrap()
    return (customers_df,)


@app.cell(hide_code=True)
def _(customers_df):
    _df = mo.sql(
        f"""
        FROM customers_df
        SELECT DISTINCT Customer
            WHERE Type <> 'HAULER'
        ORDER BY Customer
        """
    )
    return


@app.cell
def _():

    import re
    import keyword

    def _to_enum_member_name(customer: str) -> str:
        """
        Convert an arbitrary customer string into a valid, stable enum member name.
        """
        name = re.sub(r"\W+", "_", customer.strip().upper())  # spaces/punct -> _
        name = re.sub(r"^(\d)", r"NUM_\1", name)              # can't start with digit
        if keyword.iskeyword(name):
            name = name + "_"
        if not name:
            name = "EMPTY"
        return name

    def customers_str_enum(customers_df, enum_name: str = "CustomerEnum") -> type[StrEnum]:
        customers = (
            customers_df.select(pl.col("Customer").str.strip_chars()).collect().to_series().to_list()
        )

        members: dict[str, str] = {}
        used_names: set[str] = set()

        for c in customers:
            base = _to_enum_member_name(c)
            name = base
            i = 2
            while name in used_names:
                name = f"{base}_{i}"
                i += 1
            used_names.add(name)
            members[name] = c  # value is the original customer string

        return StrEnum(enum_name, members)

    return (customers_str_enum,)


@app.cell
def _(full_scows):
    full_scows.collect().schema
    return


@app.cell
def _(customers_df, customers_str_enum):
    CustomerEnum = customers_str_enum(customers_df)



    class FullScows(BaseModel):
        day_name:DayName
        date:date
        customer:CustomerEnum
        movement_type: str
        overtime: Literal["normal hours","overtime 150%","overtime 200%"]
        start_time: time
        end_time: time

        num_of_scows: int = Field(ge=0)
        storage_type: Literal["Dry","Brine"]
        tonnage: float = Field(ge=0)
        Price: float = Field(ge=0)
        total_price: float = Field(ge=0)

        @field_validator("customer", "movement_type", "overtime", "storage_type")
        @classmethod
        def non_empty(cls, v: str) -> str:
            v = v.strip()
            if not v:
                raise ValueError("must not be empty")
            return v

        @model_validator(mode="after")
        def check_time_order(self):
            if self.end_time < self.start_time:
                raise ValueError("end_time cannot be before start_time")
            return self

    return (FullScows,)


@app.cell(hide_code=True)
def _(full_scows):
    _df = mo.sql(
        f"""
        FROM full_scows
        LIMIT 10
        """
    )
    return


@app.cell
def _():
    from typing import Any

    def validate_polars_df(df: pl.DataFrame, model: type[BaseModel]):
        # Convert to row dicts. (For huge DFs, see notes below.)
        records = df.to_dicts()

        ok: list[BaseModel] = []
        errors: list[dict[str, Any]] = []

        for i, rec in enumerate(records):
            try:
                ok.append(model.model_validate(rec))
            except ValidationError as e:
                errors.append(
                    {
                        "row": i,
                        "errors": e.errors(),  # structured details (field, msg, type, etc.)
                        "input": rec,          # optional; remove if too big
                    }
                )

        return ok, errors

    return (validate_polars_df,)


@app.cell
def _(FullScows, full_scows, validate_polars_df):
    valid, errors = validate_polars_df(full_scows.collect(), FullScows)
    return (valid,)


@app.cell
def _(valid):
    pl.DataFrame([v.model_dump() for v in valid])
    return


@app.cell
def _():
    from polars_result import Ok,Err,PolarsResult
    from read_google_sheet import read_google_sheet

    return Ok, read_google_sheet


@app.cell
def _():
    misc_sheet_id = "1VbfiiWsp8yxs6KSR1CXpw1S_35tYlWV8UjjWah9Afpw"
    misc_sheet_name = "CCCSReport"

    transport_sheet_id = "1O8K26c7CqLSdLr-f2gvZliDpBn9ArxXvj9tEJy-ElUg"
    scow_transfer_sheet = "ScowTransfer"
    return (
        misc_sheet_id,
        misc_sheet_name,
        scow_transfer_sheet,
        transport_sheet_id,
    )


@app.cell
def _(
    misc_sheet_id,
    misc_sheet_name,
    read_google_sheet,
    scow_transfer_sheet,
    transport_sheet_id,
):
    bin_dispatch_dataf = read_google_sheet(sheet_id=misc_sheet_id,sheet_name=misc_sheet_name)
    scow_transfer_dataf = read_google_sheet(sheet_id=transport_sheet_id,sheet_name=scow_transfer_sheet)
    return bin_dispatch_dataf, scow_transfer_dataf


@app.function
def select_bin_dispatch(df:pl.LazyFrame):
    return df.select(
        pl.col("day").cast(pl.Enum(DayName)).alias("day_name"),
        pl.col("date"),
        pl.col("movement_type"),
        pl.col("customer"),
        pl.col("operation_type"),
        pl.col("total_tonnage").abs().cast(pl.Float64).round(3),
        pl.col("overtime_tonnage").str.replace("", "0").cast(pl.Float64).round(3),
        pl.col("storage_type")
    )


@app.function
def filter_by_year(df:pl.LazyFrame,year:int,date_col:str="date"):
    return df.filter(pl.col(date_col).dt.year().eq(year))


@app.function
def filter_by_cold_store_ops(df:pl.LazyFrame,services:list[str],service_col:str="operation_type"):
    return df.filter(pl.col(service_col).is_in(services))


@app.cell
def _(Ok, bin_dispatch_dataf):
    res = (
        bin_dispatch_dataf.and_then(lambda lf: Ok(select_bin_dispatch(lf)))
        .and_then(lambda lf: Ok(filter_by_year(lf,2026)))
        .and_then(
            lambda lf: Ok(
                filter_by_cold_store_ops(
                    lf, services=["Bin Dispatch to IOT", "Bin Dispatch from IOT"]
                )
            )
        )
    ).unwrap()
    return (res,)


@app.cell
def _(res):
    res.collect()
    return


@app.cell
def _():
    from dataclasses import dataclass


    @dataclass
    class Overtime:
        """Overtime Labels"""

        overtime_150_text:str = "overtime 150%"
        overtime_200_text:str = "overtime 200%"
        normal_hour_text:str = "normal hours"

    return (Overtime,)


@app.cell
def _(Overtime):
    def add_overtime_text(df:pl.LazyFrame,day_name_col:str,time_col:str):
        return df.with_columns(
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

    return (add_overtime_text,)


@app.cell
def _(Ok, add_overtime_text, scow_transfer_dataf):
    transfer = (scow_transfer_dataf.and_then(
        lambda lf: Ok(
            filter_by_year(lf, 2026)
            .pipe(function=lambda y: y.filter(pl.col("status").eq("Full")))
            .pipe(lambda y: y.with_columns(pl.col("date").days.add_day_name()))
            .pipe(
                lambda y: y.with_columns(
                    pl.when(pl.col("movement_type").eq("Delivery"))
                    .then(pl.col("time_out"))
                    .otherwise(pl.col("time_in"))
                    .alias("time")
                )
            )
        )
    ).and_then(
        lambda lf: Ok(
            add_overtime_text(lf, day_name_col="day_name", time_col="time")
        )
    ).unwrap())
    return (transfer,)


@app.function
def allocate_tonnage_to_transfers(
    tonnage_lf: pl.LazyFrame,
    transfers_lf: pl.LazyFrame,
) -> pl.LazyFrame:

    # --- normalize transfers to match tonnage keys ---
    transfers = (
        transfers_lf
        .with_columns(
            pl.col("date").cast(pl.Date),
            pl.col("num_of_scows").cast(pl.Int64),

            # Delivery -> OUT, Collection -> IN
            pl.when(pl.col("movement_type") == "Delivery").then(pl.lit("OUT"))
             .when(pl.col("movement_type") == "Collection").then(pl.lit("IN"))
             .otherwise(None)
             .alias("movement_type_norm"),

            # overtime flag: anything not "normal hours" is overtime
            (pl.col("overtime") != "normal hours").alias("is_overtime"),
        )
        .drop("movement_type")
        .rename({"movement_type_norm": "movement_type"})
    )

    # --- tonnage table prep ---
    tonnage = (
        tonnage_lf
        .with_columns(
            pl.col("date").cast(pl.Date),
            pl.col("total_tonnage").cast(pl.Float64),
            pl.col("overtime_tonnage").cast(pl.Float64),
        )
        .with_columns(
            (pl.col("total_tonnage") - pl.col("overtime_tonnage")).alias("normal_tonnage")
        )
    )

    # --- daily scow totals for allocation buckets ---
    daily_scows = (
        transfers
        .group_by(["date", "customer", "movement_type"])
        .agg(
            pl.col("num_of_scows").sum().alias("total_scows_all"),
            pl.col("num_of_scows").filter(pl.col("is_overtime")).sum().alias("total_scows_overtime"),
        )
        .with_columns(
            pl.col("total_scows_overtime").fill_null(0),
            (pl.col("total_scows_all") - pl.col("total_scows_overtime")).alias("total_scows_normal"),
        )
    )

    # --- join and allocate ---
    out = (
        transfers
        .join(daily_scows, on=["date", "customer", "movement_type"], how="left")
        .join(tonnage, on=["date", "customer", "movement_type"], how="left")
        .with_columns(
            # allocate overtime tonnage across overtime rows
            pl.when(pl.col("is_overtime"))
              .then(
                  pl.when(pl.col("total_scows_overtime") > 0)
                    .then(pl.col("num_of_scows") / pl.col("total_scows_overtime") * pl.col("overtime_tonnage"))
                    .otherwise(0.0)
              )
              .otherwise(0.0)
              .alias("allocated_overtime_tonnage"),

            # allocate normal tonnage across normal rows
            pl.when(~pl.col("is_overtime"))
              .then(
                  pl.when(pl.col("total_scows_normal") > 0)
                    .then(pl.col("num_of_scows") / pl.col("total_scows_normal") * pl.col("normal_tonnage"))
                    .otherwise(0.0)
              )
              .otherwise(0.0)
              .alias("allocated_normal_tonnage"),
        )
        .with_columns(
            (pl.col("allocated_overtime_tonnage") + pl.col("allocated_normal_tonnage"))
            .alias("allocated_total_tonnage")
        )
    )

    return out


@app.cell
def _(res, transfer):
    allocate_tonnage_to_transfers(tonnage_lf=res,transfers_lf=transfer).collect()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
