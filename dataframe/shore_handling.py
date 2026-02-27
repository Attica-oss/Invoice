"""Shore handling Lazyframe"""

import polars as pl

# from data_source import expressions as exp
from data.price import get_price
from data_source.make_dataset import load_gsheet_data
from data_source.sheet_ids import SHORE_HANDLING_ID, bin_tipping_sheet, salt_sheet
from type_casting import MovementType
from type_casting.customers import purseiner, ship_owner
from type_casting.dates import (
    DAY_NAMES,
    LOWER_BOUND,
    NULL_DURATION,
    SPECIAL_DAYS,
    UPPER_BOUND,
    UPPER_BOUND_SPECIAL_DAY,
    duration_to_hhmm,
    public_holiday,
)
from type_casting.validations import MovementType, OvertimePerc

BIN_TIPPING_PRICE = get_price(["CCCS Movement in/out"]).select(pl.col("Price")).to_series()[0]

SALT_PRICE = (
    get_price(["Loading (Quay to Ship)", "Loading @ Zone 14"])
    .select(pl.col("Price"))
    .to_series()[0]
)


# Public Holiday Dates

ph_list: pl.Series = public_holiday()


# Date Variables

is_special_day = pl.col("day_name").is_in(SPECIAL_DAYS)

is_not_special_day = ~is_special_day

# Condition for Duration Calculation
# For Normal Days
before_cut_off_normal_day = (pl.col("end_time") <= UPPER_BOUND) & (
    pl.col("start_time") < UPPER_BOUND
)
end_time_after_cut_off = (pl.col("end_time") > UPPER_BOUND) & (
    pl.col("start_time") < UPPER_BOUND
)  # rename variable: is_end_time_after_cut_off

start_after_cut_off_normal_day = (pl.col("end_time") > UPPER_BOUND) & (
    pl.col("start_time") >= UPPER_BOUND
)

stop_after_cut_off_normal_day = (pl.col("end_time") > UPPER_BOUND) & (
    pl.col("start_time") < UPPER_BOUND
)


# Coditional for Overtime Days

start_after_cut_off_special_day = (pl.col("end_time") > UPPER_BOUND_SPECIAL_DAY) & (
    pl.col("start_time") >= UPPER_BOUND_SPECIAL_DAY
)

stop_after_cut_off_special_day = (pl.col("end_time") > UPPER_BOUND_SPECIAL_DAY) & (
    pl.col("start_time") < UPPER_BOUND_SPECIAL_DAY
)


stop_before_cut_off_special_day = (pl.col("end_time") <= UPPER_BOUND_SPECIAL_DAY) & (
    pl.col("start_time") < UPPER_BOUND_SPECIAL_DAY
)

after_midnight: pl.Expr = (
    pl.col("end_time").lt(pl.col("start_time")).and_(pl.col("end_time").le(LOWER_BOUND))
)


# Durations based on the conditionals

hours_after_cut_off_normal_day = pl.col("date").dt.combine(pl.col("end_time")) - pl.col(
    "date"
).dt.combine(UPPER_BOUND)

hours_after_cut_off_special_day = pl.col("date").dt.combine(pl.col("end_time")) - pl.col(
    "date"
).dt.combine(UPPER_BOUND_SPECIAL_DAY)

normal_duration_special_day = pl.col("date").dt.combine(UPPER_BOUND_SPECIAL_DAY) - pl.col(
    "date"
).dt.combine(pl.col("start_time"))

normal_duration = pl.col("date").dt.combine(UPPER_BOUND) - pl.col("date").dt.combine(
    pl.col("start_time")
)


# Duration only for the portion after midnight

midnight_time = pl.lit(0)  # Midnight in seconds

duration_after_midnight = (pl.col("date").dt.combine(pl.col("end_time")) + pl.duration(days=1)) - (
    pl.col("date") + pl.duration(days=1)
).dt.combine(midnight_time)


duration_after_midnight = (pl.col("date") + pl.duration(days=1)).dt.combine(
    pl.col("end_time")
) - pl.col("date").dt.combine(pl.col("start_time"))


# For normal days with service crossing midnight
# Before upper bound portion (normal rate)
before_upper_bound_portion = (
    pl.when(after_midnight & is_not_special_day & (pl.col("start_time") < UPPER_BOUND))
    .then(pl.col("date").dt.combine(UPPER_BOUND) - pl.col("date").dt.combine(pl.col("start_time")))
    .otherwise(pl.duration())
)

# After upper bound until midnight portion (150% rate)
after_upper_bound_until_midnight = (
    pl.when(after_midnight & is_not_special_day & (pl.col("start_time") < UPPER_BOUND))
    .then(
        (pl.col("date") + pl.duration(days=1)).dt.combine(pl.lit(0))
        - pl.col("date").dt.combine(UPPER_BOUND)
    )
    .when(after_midnight & is_not_special_day & (pl.col("start_time") >= UPPER_BOUND))
    .then(
        (pl.col("date") + pl.duration(days=1)).dt.combine(pl.lit(0))
        - pl.col("date").dt.combine(pl.col("start_time"))
    )
    .otherwise(pl.duration())
)

# After midnight portion (200% rate for normal days, moved to overtime_200)
portion_after_midnight = (
    pl.when(after_midnight)
    .then(
        (pl.col("date") + pl.duration(days=1)).dt.combine(pl.col("end_time"))
        - (pl.col("date") + pl.duration(days=1)).dt.combine(pl.lit(0))
    )
    .otherwise(pl.duration())
)


durations = pl.col("date").dt.combine(pl.col("end_time")) - pl.col("date").dt.combine(
    pl.col("start_time")
)


salt: pl.LazyFrame = (
    load_gsheet_data(sheet_id=SHORE_HANDLING_ID, sheet_name=salt_sheet)
    .unwrap()
    .select(
        pl.col("day_name").cast(dtype=pl.Enum(DAY_NAMES)),
        pl.col("date"),
        pl.col("vessel").cast(dtype=pl.Enum(purseiner)),
        pl.col("customer").str.strip_chars().cast(dtype=pl.Enum(ship_owner())),
        pl.col("start_time"),
        pl.col("end_time"),
        pl.col("duration"),
        pl.col("operation_type"),
        pl.col("tonnage").cast(pl.Float64),
    )
    .with_columns(
        # Normal rate calculation
        normal=pl.when(after_midnight & is_not_special_day)
        .then(before_upper_bound_portion)
        .when(is_not_special_day & end_time_after_cut_off & ~after_midnight)
        .then(normal_duration)
        .when(is_not_special_day & before_cut_off_normal_day & ~after_midnight)
        .then(durations)
        .otherwise(pl.duration()),
        # 150% rate calculation - only until midnight for normal days
        normal_150=pl.when(after_midnight & is_not_special_day)
        .then(after_upper_bound_until_midnight)  # Only until midnight
        .when(is_not_special_day & start_after_cut_off_normal_day & ~after_midnight)
        .then(durations)
        .when(is_not_special_day & stop_after_cut_off_normal_day & ~after_midnight)
        .then(hours_after_cut_off_normal_day)
        .otherwise(pl.duration()),
        # Special days at 150%
        sun_150=pl.when(is_special_day & stop_after_cut_off_special_day & ~after_midnight)
        .then(normal_duration_special_day)
        .when(is_special_day & stop_before_cut_off_special_day & ~after_midnight)
        .then(durations)
        .when(is_special_day & after_midnight & (pl.col("start_time") < UPPER_BOUND_SPECIAL_DAY))
        .then(
            pl.col("date").dt.combine(UPPER_BOUND_SPECIAL_DAY)
            - pl.col("date").dt.combine(pl.col("start_time"))
        )
        .otherwise(pl.duration()),
        # 200% overtime - including after midnight for normal days
        overtime_200=pl.when(is_special_day & start_after_cut_off_special_day & ~after_midnight)
        .then(durations)
        .when(is_special_day & stop_after_cut_off_special_day & ~after_midnight)
        .then(hours_after_cut_off_special_day)
        .when(is_special_day & after_midnight)
        .then(
            pl.when(pl.col("start_time") >= UPPER_BOUND_SPECIAL_DAY)
            .then(duration_after_midnight)
            .otherwise(
                (pl.col("date") + pl.duration(days=1)).dt.combine(pl.col("end_time"))
                - pl.col("date").dt.combine(UPPER_BOUND_SPECIAL_DAY)
            )
        )
        .when(is_not_special_day & after_midnight)  # Add this crucial condition for normal days
        .then(portion_after_midnight)  # After midnight portion goes to 200% for normal days
        .otherwise(pl.duration()),
        # Add durations column for after midnight case
        total_duration=pl.when(after_midnight).then(duration_after_midnight).otherwise(durations),
    )
    .with_columns(
        # Calculate weighted values by tonnage, using total_duration for after midnight cases
        normal=(
            pl.col("normal")
            / pl.when(after_midnight).then(pl.col("total_duration")).otherwise(durations)
        )
        * pl.col("tonnage"),
        overtime_150=(
            pl.col("normal_150")
            / pl.when(after_midnight).then(pl.col("total_duration")).otherwise(durations)
        )
        * pl.col("tonnage")
        + (
            pl.col("sun_150")
            / pl.when(after_midnight).then(pl.col("total_duration")).otherwise(durations)
        )
        * pl.col("tonnage"),
        overtime_200=(
            pl.col("overtime_200")
            / pl.when(after_midnight).then(pl.col("total_duration")).otherwise(durations)
        )
        * pl.col("tonnage"),
    )
    .with_columns(
        price=(pl.col("normal") * SALT_PRICE * OvertimePerc.normal_hour)
        + (pl.col("overtime_150") * SALT_PRICE * OvertimePerc.overtime_150)
        + (pl.col("overtime_200") * SALT_PRICE * OvertimePerc.overtime_200)
    )
    .select(pl.all().exclude(["normal_150", "sun_150", "total_duration"]))
)


def forklift_salt() -> pl.LazyFrame:
    """Forklift for salt operation"""
    df = salt
    result = (
        df.filter(pl.col("operation_type").ne(pl.lit("Loading @ Zone 14", dtype=pl.Utf8)))
        .select(
            pl.col("day_name").alias("day"),
            pl.col("date"),
            pl.col("vessel"),
            pl.col("customer"),
            pl.col("start_time"),
            pl.col("end_time"),
            (
                pl.lit("Salt Loading (")
                + pl.col("tonnage").cast(pl.Int64).cast(pl.Utf8)
                + pl.lit(" ) Tons")
            ).alias("purpose"),
        )
        .with_columns(
            # Calculate total duration accounting for after midnight services
            total_duration=pl.when(after_midnight)
            .then(duration_after_midnight)
            .otherwise(
                pl.col("date").dt.combine(pl.col("end_time"))
                - pl.col("date").dt.combine(pl.col("start_time"))
            ),
            # Overtime for normal services (non-special days)
            overtime_for_normal_services=pl.when(
                # After midnight on normal days - handle portion before midnight
                (after_midnight)
                & (~pl.col("day").is_in(SPECIAL_DAYS))
                & (pl.col("start_time") > UPPER_BOUND)
            )
            .then(
                (pl.col("date") + pl.duration(days=1)).dt.combine(pl.lit(0))
                - pl.col("date").dt.combine(pl.col("start_time"))
            )
            .when(
                (after_midnight)
                & (~pl.col("day").is_in(SPECIAL_DAYS))
                & (pl.col("start_time") <= UPPER_BOUND)
            )
            .then(
                (pl.col("date") + pl.duration(days=1)).dt.combine(pl.lit(0))
                - pl.col("date").dt.combine(UPPER_BOUND)
            )
            .when(
                (~pl.col("day").is_in(SPECIAL_DAYS))
                & (pl.col("end_time") > UPPER_BOUND)
                & (pl.col("start_time") > UPPER_BOUND)
                & (~after_midnight)
            )
            .then(
                pl.col("date").dt.combine(pl.col("end_time"))
                - pl.col("date").dt.combine(pl.col("start_time"))
            )
            .when(
                (~pl.col("day").is_in(SPECIAL_DAYS))
                & (pl.col("end_time") > UPPER_BOUND)
                & (~after_midnight)
            )
            .then(
                pl.col("date").dt.combine(pl.col("end_time"))
                - pl.col("date").dt.combine(UPPER_BOUND)
            )
            .when(
                (pl.col("day").is_in(SPECIAL_DAYS))
                & (pl.col("end_time") < UPPER_BOUND_SPECIAL_DAY)
                & (pl.col("start_time") < UPPER_BOUND_SPECIAL_DAY)
                & (~after_midnight)
            )
            .then(
                pl.col("date").dt.combine(pl.col("end_time"))
                - pl.col("date").dt.combine(pl.col("start_time"))
            )
            .when(
                (pl.col("day").is_in(SPECIAL_DAYS))
                & (pl.col("end_time") > UPPER_BOUND_SPECIAL_DAY)
                & (pl.col("start_time") < UPPER_BOUND_SPECIAL_DAY)
                & (~after_midnight)
            )
            .then(
                pl.col("date").dt.combine(UPPER_BOUND_SPECIAL_DAY)
                - pl.col("date").dt.combine(pl.col("start_time"))
            )
            .when(
                (after_midnight)
                & (pl.col("day").is_in(SPECIAL_DAYS))
                & (pl.col("start_time") < UPPER_BOUND_SPECIAL_DAY)
            )
            .then(
                pl.col("date").dt.combine(UPPER_BOUND_SPECIAL_DAY)
                - pl.col("date").dt.combine(pl.col("start_time"))
            )
            .otherwise(NULL_DURATION),
            # Overtime for extended services (special days after cutoff and all after-midnight portions)
            overtime_for_extended_services=pl.when(
                # After midnight portion on normal days (goes to 200%)
                (after_midnight) & (~pl.col("day").is_in(SPECIAL_DAYS))
            )
            .then(
                (pl.col("date") + pl.duration(days=1)).dt.combine(pl.col("end_time"))
                - (pl.col("date") + pl.duration(days=1)).dt.combine(pl.lit(0))
            )
            .when(
                (after_midnight)
                & (pl.col("day").is_in(SPECIAL_DAYS))
                & (pl.col("start_time") >= UPPER_BOUND_SPECIAL_DAY)
            )
            .then(duration_after_midnight)
            .when(
                (after_midnight)
                & (pl.col("day").is_in(SPECIAL_DAYS))
                & (pl.col("start_time") < UPPER_BOUND_SPECIAL_DAY)
            )
            .then(
                (pl.col("date") + pl.duration(days=1)).dt.combine(pl.col("end_time"))
                - pl.col("date").dt.combine(UPPER_BOUND_SPECIAL_DAY)
            )
            .when(
                (pl.col("day").is_in(SPECIAL_DAYS))
                & (pl.col("end_time") > UPPER_BOUND_SPECIAL_DAY)
                & (pl.col("start_time") > UPPER_BOUND_SPECIAL_DAY)
                & (~after_midnight)
            )
            .then(
                pl.col("date").dt.combine(pl.col("end_time"))
                - pl.col("date").dt.combine(pl.col("start_time"))
            )
            .when(
                (pl.col("day").is_in(SPECIAL_DAYS))
                & (pl.col("end_time") > UPPER_BOUND_SPECIAL_DAY)
                & (~after_midnight)
            )
            .then(
                pl.col("date").dt.combine(pl.col("end_time"))
                - pl.col("date").dt.combine(UPPER_BOUND_SPECIAL_DAY)
            )
            .otherwise(NULL_DURATION),
        )
        .with_columns(
            # Calculate normal hour services as total minus both overtime categories
            normal_hour_services=pl.when(after_midnight)
            .then(
                pl.when(~pl.col("day").is_in(SPECIAL_DAYS) & (pl.col("start_time") < UPPER_BOUND))
                .then(
                    pl.col("date").dt.combine(UPPER_BOUND)
                    - pl.col("date").dt.combine(pl.col("start_time"))
                )
                .when(
                    pl.col("day").is_in(SPECIAL_DAYS)
                    & (pl.col("start_time") < UPPER_BOUND_SPECIAL_DAY)
                )
                .then(
                    pl.col("date").dt.combine(UPPER_BOUND_SPECIAL_DAY)
                    - pl.col("date").dt.combine(pl.col("start_time"))
                )
                .otherwise(NULL_DURATION)
            )
            .otherwise(
                pl.col("total_duration")
                - (
                    pl.col("overtime_for_normal_services")
                    + pl.col("overtime_for_extended_services")
                )
            )
        )
    )

    # Format duration to HH:MM format
    duration_columns = [
        "total_duration",
        "overtime_for_normal_services",
        "overtime_for_extended_services",
        "normal_hour_services",
    ]

    return duration_to_hhmm(result, duration_columns)


add_day_name_col: pl.Expr = (
    pl.when(pl.col("Date").is_in(ph_list))
    .then(pl.lit("PH"))
    .otherwise(pl.col("Date").dt.to_string(format="%a"))
)


bin_tipping: pl.LazyFrame = (
    load_gsheet_data(sheet_id=SHORE_HANDLING_ID, sheet_name=bin_tipping_sheet)
    .unwrap()
    .filter(pl.col("Tonnage Tipped") > 0)
    .with_columns(day_name=add_day_name_col, Service=pl.lit("IPHS Bin Tipping"))
    .select(
        pl.col("day_name").cast(dtype=pl.Enum(DAY_NAMES)),
        pl.col("Date"),
        pl.col("Customer"),
        pl.col("movement_type").cast(dtype=MovementType.enum_dtype()),
        pl.col("Service"),
        pl.col("IOT Scows (Tipping)").alias("number_of_scows_tipped"),
        pl.col("Tonnage Tipped").cast(pl.Float64),
        pl.col("Overtime"),
    )
    .with_columns(
        price=BIN_TIPPING_PRICE,
        total_price=pl.when(pl.col("day_name").is_in(SPECIAL_DAYS))
        .then(
            (
                pl.col("price")
                * OvertimePerc.overtime_150
                * (pl.col("Tonnage Tipped") - pl.col("Overtime"))
            )
            + (pl.col("price") * OvertimePerc.overtime_200 * pl.col("Overtime"))
        )
        .otherwise(
            (
                pl.col("price")
                * OvertimePerc.normal_hour
                * (pl.col("Tonnage Tipped") - pl.col("Overtime"))
            )
            + (pl.col("price") * OvertimePerc.overtime_150 * pl.col("Overtime"))
        ),
    )
)
