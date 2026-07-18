import marimo

__generated_with = "0.23.1"
app = marimo.App(width="columns")

with app.setup:
    from __future__ import annotations

    import polars as pl
    import marimo as mo

    from dataframe import forklift_for_salt,salt



 
    from datetime import time as dt_time
    from typing import Iterable, Mapping

    from data.price import get_price
    from data_source.make_dataset import load_gsheet_data
    from data_source.sheet_ids import SALT_OPERATION_SHEET_NAME, SHORE_HANDLING_ID
    from type_casting.customers import purseiner, ship_owner
    from type_casting.dates import (
        CURRENT_YEAR,
        SPECIAL_DAYS,
        UPPER_BOUND,
        UPPER_BOUND_SPECIAL_DAY,
        duration_to_hhmm,
    )



    from type_casting.validations import OvertimePerc


    ZERO_DURATION = pl.duration(seconds=0)
    MIDNIGHT = pl.time(0, 0,0)  # dt.combine needs a Time expr, not pl.lit(0)


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.function
# ---------------------------------------------------------------------------
# Rate-bucket splitting (single source of truth)
# ---------------------------------------------------------------------------
 
 
def rate_bucket_exprs(
    *,
    upper_bound: dt_time = UPPER_BOUND,
    upper_bound_special: dt_time = UPPER_BOUND_SPECIAL_DAY,
    special_days: Iterable[str] = SPECIAL_DAYS,
    day_col: str = "day_name",
) -> Mapping[str, pl.Expr]:
    """Expressions splitting a service interval into billing-rate buckets.
 
    Expects the frame to carry ``date`` (Date), ``start_time`` and
    ``end_time`` (Time), and a day-name column (``day_col``) whose values
    include the entries of ``special_days`` (e.g. "Sat", "Sun", "PH").
 
    Business rules encoded
    ----------------------
    * ``end_time < start_time``  =>  the service crosses midnight into the
      next calendar day.
    * Normal day:   start .. cutoff        -> normal rate
                    cutoff .. midnight     -> 150 %
                    midnight .. end        -> 200 %
    * Special day:  start .. special cutoff-> 150 %
                    special cutoff .. end  -> 200 %  (incl. after midnight)
 
    Returned columns: ``total_duration``, ``normal``, ``overtime_150``,
    ``overtime_200`` — all Durations, never null, and always satisfying
    ``total_duration == normal + overtime_150 + overtime_200``.
    """
    crossed_midnight = pl.col("end_time") < pl.col("start_time")
 
    start = pl.col("date").dt.combine(pl.col("start_time"))
    end = (
        pl.when(crossed_midnight)
        .then((pl.col("date") + pl.duration(days=1)).dt.combine(pl.col("end_time")))
        .otherwise(pl.col("date").dt.combine(pl.col("end_time")))
    )
 
    is_special = pl.col(day_col).is_in(list(special_days))
    cutoff = pl.col("date").dt.combine(
        pl.when(is_special)
        .then(pl.lit(upper_bound_special))
        .otherwise(pl.lit(upper_bound))
    )
    midnight = (pl.col("date") + pl.duration(days=1)).dt.combine(MIDNIGHT)
 
    def segment(lo: pl.Expr, hi: pl.Expr) -> pl.Expr:
        """Length of the service interval clipped to the window [lo, hi)."""
        overlap = pl.min_horizontal(end, hi) - pl.max_horizontal(start, lo)
        return pl.max_horizontal(overlap, ZERO_DURATION)
 
    pre_cutoff = segment(start, cutoff)
    cutoff_to_midnight = segment(cutoff, midnight)
    after_midnight = segment(midnight, end)
 
    return {
        "total_duration": end - start,
        "normal": pl.when(is_special).then(ZERO_DURATION).otherwise(pre_cutoff),
        # Covers both the old ``normal_150`` (normal-day evening) and
        # ``sun_150`` (special-day daytime) columns.
        "overtime_150": pl.when(is_special)
        .then(pre_cutoff)
        .otherwise(cutoff_to_midnight),
        "overtime_200": after_midnight
        + pl.when(is_special).then(cutoff_to_midnight).otherwise(ZERO_DURATION),
    }


@app.cell
def _():
    return


@app.cell
def _():
    # ---------------------------------------------------------------------------
    # Pricing frame
    # ---------------------------------------------------------------------------
 
 
    def _salt_unit_price() -> float:
        # The old code fetched two rows ("Loading (Quay to Ship)" and
        # "Loading @ Zone 14") and took whichever came first. Select the row
        # explicitly; if Zone 14 loading has its own price, price it per
        # operation_type instead of with a single scalar.
        return get_price(["Loading (Quay to Ship)"]).select("unit_price").item()
 
 
    def load_salt() -> pl.LazyFrame:
        """Salt-operation services for the current year.
 
        Output columns: the raw service columns plus Duration buckets
        (``total_duration``, ``normal``, ``overtime_150``, ``overtime_200``),
        tonnage split proportionally to time in each bucket
        (``*_tonnage``), and ``price``.
        """
        salt_price = _salt_unit_price()
 
        def tonnage_share(bucket: str) -> pl.Expr:
            # Guard zero-length services: 0/0 would otherwise produce NaN
            # that flows straight into ``price``.
            return (
                pl.when(pl.col("total_duration") > ZERO_DURATION)
                .then(pl.col(bucket) / pl.col("total_duration"))
                .otherwise(0.0)
            ) * pl.col("tonnage")
 
        return (
            load_gsheet_data(
                sheet_id=SHORE_HANDLING_ID, sheet_name=SALT_OPERATION_SHEET_NAME
            )
            .filter(
                pl.col("date").dt.year().eq(CURRENT_YEAR),
                pl.col("start_time").is_not_null(),
                pl.col("end_time").is_not_null(),
            )
            .select(
                pl.col("date").days.add_day_name(),
                pl.col("date"),
                pl.col("vessel").cast(pl.Enum(purseiner)),
                pl.col("customer").str.strip_chars().cast(pl.Enum(ship_owner())),
                pl.col("start_time"),
                pl.col("end_time"),
                pl.col("duration"),
                pl.col("operation_type"),
                pl.col("tonnage").cast(pl.Float64, strict=False),
            )
            .with_columns(**rate_bucket_exprs())
            .with_columns(
                normal_tonnage=tonnage_share("normal"),
                overtime_150_tonnage=tonnage_share("overtime_150"),
                overtime_200_tonnage=tonnage_share("overtime_200"),
            )
            .with_columns(
                price=salt_price
                * (
                    pl.col("normal_tonnage") * OvertimePerc.normal_hour
                    + pl.col("overtime_150_tonnage") * OvertimePerc.overtime_150
                    + pl.col("overtime_200_tonnage") * OvertimePerc.overtime_200
                )
            )
        )
 

    return (load_salt,)


@app.cell
def _(load_salt):

 
    def forklift_salt(salt: pl.LazyFrame | None = None) -> pl.LazyFrame:
        """Forklift report for salt operations, durations formatted as HH:MM.
 
        Reuses the Duration buckets computed in :func:`load_salt` instead of
        re-deriving them, so report and billing always agree.
        """
        lf = salt if salt is not None else load_salt()
 
        result = lf.filter(
            # ne_missing keeps rows with a null operation_type — plain ``ne``
            # evaluated to null and silently dropped them.
            pl.col("operation_type").ne_missing("Loading @ Zone 14")
        ).select(
            pl.col("day_name").alias("day"),
            "date",
            "vessel",
            "customer",
            "start_time",
            "end_time",
            pl.format("Salt Loading ({} Tons)", pl.col("tonnage").cast(pl.Int64)).alias(
                "purpose"
            ),
            "total_duration",
            pl.col("normal").alias("normal_hour_services"),
            pl.col("overtime_150").alias("overtime_for_normal_services"),
            pl.col("overtime_200").alias("overtime_for_extended_services"),
        )
 
        return duration_to_hhmm(
            result,
            [
                "total_duration",
                "normal_hour_services",
                "overtime_for_normal_services",
                "overtime_for_extended_services",
            ],
        )

    return


@app.cell(hide_code=True)
def _():
    _df = mo.sql(
        f"""
        FROM salt
        """
    )
    return


@app.cell
def _(load_salt):
    load_salt().collect()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
