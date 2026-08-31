"""
dates.py: Date and time utilities for shore-handling pipelines.

Provides:
- Time-of-day and duration constants (plain ``datetime.time`` values,
  wrapped in Polars expressions at the use site)
- ``DayName`` enum and the ``.days`` expression namespace
- Public-holiday calendar (fixed, continuous, one-time and Easter-related
  holidays, with the Monday-after-Sunday rule)
- ``duration_to_hhmm`` for formatting Duration columns as "HH:MM" strings

Rewrite notes
-------------
* ``duration_to_hhmm`` now returns *strings* (as its docstring always
  claimed). The old ``str.to_time`` cast broke on any total >= 24 h.
* Holidays are no longer hardcoded to 2026: ``public_holiday`` covers a
  window of years around ``CURRENT_YEAR``, so rows from adjacent years
  are still classified as "PH" (this drives the 150/200 % rates).
* ``Days.add_day_name`` checks against a materialized ``pl.Series`` —
  ``LazyFrame.implode()`` doesn't exist and raised at first use.
* Time constants are plain ``datetime.time`` instead of ``pl.Expr``:
  usable in ``pl.lit``, in tests, and in non-Polars code.
* ``NULL_DURATION`` is gone — it was zero, and the name invited exactly
  the null-propagation confusion it caused downstream. Use
  ``ZERO_DURATION``.
* ``LOWER_BOUND`` is 08:00 sharp; compare with strict ``<`` instead of
  ``<=`` against 07:59:59 (which left a one-second undefined gap).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, time, timedelta

import polars as pl

from utils.polars_enum import PolarsEnum

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CURRENT_YEAR: int = date.today().year

# Years for which holiday dates are materialized. Covers the previous and
# next year so that year-boundary data is still classified correctly.
HOLIDAY_YEAR_WINDOW: range = range(CURRENT_YEAR - 1, CURRENT_YEAR + 2)

ZERO_DURATION: pl.Expr = pl.duration(seconds=0)

MIDNIGHT: time = time(0, 0)
UPPER_BOUND: time = time(17, 0)  # normal-day overtime cutoff
UPPER_BOUND_SPECIAL_DAY: time = time(16, 0)  # special-day overtime cutoff
LOWER_BOUND: time = time(8, 0)  # start of the working day; use strict <


# ---------------------------------------------------------------------------
# Day names
# ---------------------------------------------------------------------------


class DayName(PolarsEnum):
    """Enum for day names with Polars integration.

    Note: enum order defines sort order for the ``day_name`` column.
    """

    PH = "PH"
    SUN = "Sun"
    MON = "Mon"
    TUE = "Tue"
    WED = "Wed"
    THU = "Thu"
    FRI = "Fri"
    SAT = "Sat"

    @classmethod
    def special_days(cls) -> list[DayName]:
        """Days billed at special (150 %+) rates from the first hour."""
        return [cls.SUN, cls.PH]


# String values, safe to pass to ``pl.Expr.is_in``.
SPECIAL_DAYS: list[str] = [day.value for day in DayName.special_days()]


# ---------------------------------------------------------------------------
# Public holidays
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ContinuousHoliday:
    """A holiday that recurs every year from ``start_year`` onwards."""

    start_year: int
    month: int
    day: int
    name: str

    def applies(self, year: int) -> bool:
        return year >= self.start_year

    def to_date(self, year: int) -> date:
        return date(year, self.month, self.day)


NEW_CONTINUOUS_HOLIDAYS: list[ContinuousHoliday] = [
    ContinuousHoliday(start_year=2026, month=2, day=1, name="Abolition of Slavery"),
]

ONE_TIME_HOLIDAYS_BY_YEAR: dict[int, set[date]] = {
    2025: {
        date(2025, 10, 11),
        date(2025, 10, 13),
        date(2025, 10, 27),
    },
    2026: {
        date(2026, 6, 30),
    },
}

FIXED_HOLIDAYS: list[tuple[int, int]] = [
    (1, 1),
    (1, 2),
    (5, 1),
    (6, 18),
    (6, 29),
    (8, 15),
    (11, 1),
    (12, 8),
    (12, 25),
]


@dataclass(frozen=True, slots=True)
class PublicHolidayCalendar:
    fixed_holidays: list[tuple[int, int]]
    continuous_holidays: list[ContinuousHoliday]
    one_time_holidays_by_year: dict[int, set[date]]

    def get_fixed_holidays(self, year: int) -> set[date]:
        return {date(year, month, day) for month, day in self.fixed_holidays}

    def get_continuous_holidays(self, year: int) -> set[date]:
        return {
            holiday.to_date(year) for holiday in self.continuous_holidays if holiday.applies(year)
        }

    def get_one_time_holidays(self, year: int) -> set[date]:
        return self.one_time_holidays_by_year.get(year, set())

    def get_easter_related_holidays(self, year: int) -> set[date]:
        easter = self._calculate_easter_sunday(year)
        return {
            easter,  # Easter Sunday
            easter - timedelta(days=2),  # Good Friday
            easter - timedelta(days=1),  # Holy Saturday
            easter + timedelta(days=1),  # Easter Monday
            easter + timedelta(days=60),  # Corpus Christi
        }

    def get_holidays(self, year: int) -> set[date]:
        holidays: set[date] = set()
        holidays |= self.get_fixed_holidays(year)
        holidays |= self.get_continuous_holidays(year)
        holidays |= self.get_one_time_holidays(year)
        holidays |= self.get_easter_related_holidays(year)

        # Applied to the union so every Sunday holiday, whatever its
        # source, rolls over to Monday (duplicates deduplicate via set).
        holidays |= self._get_monday_after_sunday_holidays(holidays)
        return holidays

    def get_holidays_for_years(self, years: Iterable[int]) -> list[date]:
        return sorted({d for year in years for d in self.get_holidays(year)})

    def to_series(self, years: Iterable[int]) -> pl.Series:
        return pl.Series("date", self.get_holidays_for_years(years), dtype=pl.Date)

    def to_lazyframe(self, years: Iterable[int]) -> pl.LazyFrame:
        return pl.LazyFrame(
            {"date": self.get_holidays_for_years(years)},
            schema={"date": pl.Date},
        ).with_columns(pl.lit("PH").alias("day_name"))

    @staticmethod
    def _get_monday_after_sunday_holidays(holidays: set[date]) -> set[date]:
        return {holiday + timedelta(days=1) for holiday in holidays if holiday.weekday() == 6}

    @staticmethod
    def _calculate_easter_sunday(year: int) -> date:
        """Meeus/Jones/Butcher Gregorian computus."""
        a = year % 19
        b = year // 100
        c = year % 100
        d = (19 * a + b - b // 4 - ((b - (b + 8) // 25 + 1) // 3) + 15) % 30
        e = (32 + 2 * (b % 4) + 2 * (c // 4) - d - (c % 4)) % 7
        f = d + e - 7 * ((a + 11 * d + 22 * e) // 451) + 114
        return date(year, f // 31, f % 31 + 1)


HOLIDAY_CALENDAR = PublicHolidayCalendar(
    fixed_holidays=FIXED_HOLIDAYS,
    continuous_holidays=NEW_CONTINUOUS_HOLIDAYS,
    one_time_holidays_by_year=ONE_TIME_HOLIDAYS_BY_YEAR,
)


def public_holiday(years: Iterable[int] = HOLIDAY_YEAR_WINDOW) -> pl.Series:
    """Public-holiday dates for ``years`` (default: around CURRENT_YEAR)."""
    return HOLIDAY_CALENDAR.to_series(years)


# Materialized once; cheap (a few dozen dates) and used by the ``days``
# expression namespace below.
_PUBLIC_HOLIDAY_DATES: pl.Series = public_holiday()


# ---------------------------------------------------------------------------
# Expression namespace
# ---------------------------------------------------------------------------


@pl.api.register_expr_namespace("days")
class Days:
    """``pl.col("date").days.*`` helpers."""

    def __init__(self, expr: pl.Expr) -> None:
        self._expr = expr

    def add_day_name(self) -> pl.Expr:
        """Day name of a Date expression: "Mon" .. "Sun", or "PH" for a
        public holiday. Returns a ``day_name`` column typed as the
        ``DayName`` enum."""
        return (
            # .implode(): required by Polars >= 1.30 when checking
            # membership against a whole Series.
            pl.when(self._expr.is_in(_PUBLIC_HOLIDAY_DATES.implode()))
            .then(pl.lit("PH"))
            .otherwise(self._expr.dt.to_string(format="%a"))
            .cast(DayName.enum_dtype())
            .alias("day_name")
        )


# ---------------------------------------------------------------------------
# Duration formatting
# ---------------------------------------------------------------------------

type FrameT = pl.DataFrame | pl.LazyFrame


def duration_to_hhmm(
    df: FrameT,
    duration_columns: str | list[str] | None = None,
) -> FrameT:
    """Convert Duration columns to "HH:MM" strings.

    Hours are *total* hours, so values of 24 h and above render correctly
    (e.g. ``26:45``). Nulls stay null. Negative durations render with a
    leading minus (e.g. ``-01:30``) — if you see one, upstream interval
    logic is wrong.

    Args:
        df: DataFrame or LazyFrame.
        duration_columns: Column name(s) to convert. If None, converts
            every Duration column in the schema.

    Returns:
        Same frame type, with the selected columns as Utf8 strings.
    """
    schema = df.collect_schema()

    if duration_columns is None:
        duration_columns: list[str] = [
            name for name, dtype in schema.items() if isinstance(dtype, pl.Duration)
        ]
    elif isinstance(duration_columns, str):
        duration_columns: list[str] = [duration_columns]

    missing = [c for c in duration_columns if c not in schema]
    if missing:
        raise ValueError(f"duration_to_hhmm: columns not in frame: {missing}")

    if not duration_columns:
        return df

    def hhmm_expr(col: str) -> pl.Expr:
        total_minutes = pl.col(col).dt.total_minutes()
        sign = pl.when(total_minutes < 0).then(pl.lit("-")).otherwise(pl.lit(""))
        magnitude = total_minutes.abs()
        hours = magnitude.floordiv(60).cast(pl.Utf8).str.zfill(2)
        minutes = magnitude.mod(60).cast(pl.Utf8).str.zfill(2)
        # when/then propagates null for null inputs automatically.
        return (sign + hours + pl.lit(":") + minutes).alias(col)

    return df.with_columns([hhmm_expr(col) for col in duration_columns])
