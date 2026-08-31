"""Pure-logic tests for type_casting.dates.

No network: every symbol here is computed from the stdlib calendar plus a
hardcoded holiday table.
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

# Importing type_casting.dates also registers the `days` expression namespace.
from type_casting.dates import (
    CURRENT_YEAR,
    NULL_DURATION,
    ZERO_DURATION,
    DayName,
    Month,
    Year,
    duration_to_hhmm,
    get_2_months_range,
    month_range,
    public_holiday,
)

# --------------------------------------------------------------------------
# Year
# --------------------------------------------------------------------------


def test_year_accepts_in_range_and_is_int():
    y = Year(2026)
    assert y == 2026
    assert isinstance(y, int)


@pytest.mark.parametrize("bad", [1999, 3001, 0])
def test_year_rejects_out_of_range(bad):
    with pytest.raises(ValueError):
        Year(bad)


def test_year_date_range_for_a_year():
    assert Year.date_range_for_a_year(2026) == (date(2026, 1, 1), date(2026, 12, 31))


# --------------------------------------------------------------------------
# Month
# --------------------------------------------------------------------------


def test_month_name_number_roundtrip():
    assert Month.to_number("MARCH") == 3
    assert Month.from_number(3) is Month.MARCH
    assert Month.from_name("march") is Month.MARCH


def test_month_range_spans_full_month():
    assert month_range("FEBRUARY", Year(2026)) == (date(2026, 2, 1), date(2026, 2, 28))
    assert month_range("DECEMBER", Year(2024)) == (date(2024, 12, 1), date(2024, 12, 31))


def test_get_2_months_range_includes_previous_month():
    start, end = get_2_months_range("MARCH")
    assert start == date(CURRENT_YEAR, 2, 1)
    assert end == month_range("MARCH")[1]


# --------------------------------------------------------------------------
# DayName
# --------------------------------------------------------------------------


def test_dayname_special_days_and_enum_dtype():
    assert DayName.special_days() == [DayName.SUN, DayName.PH]
    assert DayName.implode() == ["Sun", "PH"]
    dtype = DayName.enum_dtype()
    assert isinstance(dtype, pl.Enum)
    assert dtype.categories.to_list() == ["PH", "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]


# --------------------------------------------------------------------------
# Public holidays
# --------------------------------------------------------------------------


def test_public_holiday_contains_fixed_and_easter_dates():
    holidays = set(public_holiday(2026).to_list())
    # Fixed
    assert date(2026, 1, 1) in holidays
    assert date(2026, 12, 25) in holidays
    # Easter 2026 is Sunday 5 April; Good Friday / Easter Monday around it.
    assert date(2026, 4, 5) in holidays
    assert date(2026, 4, 3) in holidays
    assert date(2026, 4, 6) in holidays


def test_public_holiday_one_time_entry():
    assert date(2026, 6, 30) in set(public_holiday(2026).to_list())
    assert date(2026, 6, 30) not in set(public_holiday(2024).to_list())


def test_continuous_holiday_starts_in_2026_only():
    assert date(2025, 2, 1) not in set(public_holiday(2025).to_list())
    assert date(2026, 2, 1) in set(public_holiday(2026).to_list())


def test_monday_after_sunday_holiday_is_added():
    # 1 Feb 2026 (continuous holiday) falls on a Sunday -> 2 Feb rolls in.
    assert date(2026, 2, 1).weekday() == 6
    assert date(2026, 2, 2) in set(public_holiday(2026).to_list())


# --------------------------------------------------------------------------
# .days.add_day_name  (registered expression namespace)
# --------------------------------------------------------------------------


def test_add_day_name_marks_public_holiday_and_weekday():
    normal = date(2026, 4, 15)  # a plain Wednesday, clear of Easter
    holiday = date(2026, 6, 29)  # fixed public holiday
    out = pl.DataFrame({"date": [normal, holiday]}).with_columns(
        pl.col("date").days.add_day_name()
    )
    assert out["day_name"].to_list() == [normal.strftime("%a"), "PH"]
    assert out.schema["day_name"] == DayName.enum_dtype()


# --------------------------------------------------------------------------
# duration_to_hhmm
# --------------------------------------------------------------------------


def _durations(values: list) -> pl.DataFrame:
    return pl.DataFrame({"d": pl.Series(values, dtype=pl.Duration("us"))})


def test_duration_to_hhmm_handles_over_24h_negative_and_null():
    df = _durations(
        [
            timedelta(hours=26, minutes=45),
            timedelta(hours=1, minutes=5),
            timedelta(hours=-1, minutes=-30),
            None,
        ]
    )
    out = duration_to_hhmm(df, "d")
    assert out.schema["d"] == pl.String
    assert out["d"].to_list() == ["26:45", "01:05", "-01:30", None]


def test_duration_to_hhmm_accepts_str_or_list_and_autodetects():
    df = _durations([timedelta(minutes=90)])
    assert duration_to_hhmm(df, "d")["d"].to_list() == ["01:30"]
    assert duration_to_hhmm(df, ["d"])["d"].to_list() == ["01:30"]
    assert duration_to_hhmm(df)["d"].to_list() == ["01:30"]  # auto-detect


def test_duration_to_hhmm_no_duration_columns_is_a_noop():
    df = pl.DataFrame({"x": [1, 2]})
    assert duration_to_hhmm(df).equals(df)


def test_duration_to_hhmm_missing_column_raises():
    df = _durations([timedelta(minutes=1)])
    with pytest.raises(ValueError, match="not in frame"):
        duration_to_hhmm(df, "nope")


def test_duration_to_hhmm_is_lazy_preserving():
    lf = _durations([timedelta(minutes=1)]).lazy()
    out = duration_to_hhmm(lf, "d")
    assert isinstance(out, pl.LazyFrame)
    assert out.collect()["d"].to_list() == ["00:01"]


def test_zero_duration_alias():
    assert ZERO_DURATION is NULL_DURATION
    val = pl.select(ZERO_DURATION).to_series()[0]
    assert val.total_seconds() == 0
