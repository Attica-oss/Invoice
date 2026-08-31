"""Tests for type_casting.cast_to_numbers.CastToNumbers."""

from __future__ import annotations

import polars as pl
import pytest

from type_casting.cast_to_numbers import CastToNumbers, Numbers

_RAW = pl.DataFrame({"v": ["1.5", "  2 ", "", None, "3"]})


def test_to_float64_strips_and_zero_fills_blanks():
    out = _RAW.select(CastToNumbers("v").to_numbers(Numbers.FLOAT64)).to_series()
    assert out.dtype == pl.Float64
    assert out.to_list() == [1.5, 2.0, 0.0, 0.0, 3.0]


def test_to_int64_zero_fills_blanks():
    df = pl.DataFrame({"v": ["5", "", "10", None]})
    out = df.select(CastToNumbers("v").to_numbers(Numbers.INT64)).to_series()
    assert out.dtype == pl.Int64
    assert out.to_list() == [5, 0, 10, 0]


@pytest.mark.parametrize(
    ("member", "dtype"),
    [
        (Numbers.INT8, pl.Int8),
        (Numbers.INT16, pl.Int16),
        (Numbers.INT32, pl.Int32),
    ],
)
def test_small_int_widths(member, dtype):
    df = pl.DataFrame({"v": ["7", ""]})
    out = df.select(CastToNumbers("v").to_numbers(member)).to_series()
    assert out.dtype == dtype
    assert out.to_list() == [7, 0]


def test_decimal_uses_precision_and_scale():
    out = _RAW.select(
        CastToNumbers("v").to_numbers(Numbers.DECIMAL, precision=10, scale=2)
    ).to_series()
    assert out.dtype == pl.Decimal(precision=10, scale=2)
    assert [str(x) for x in out.to_list()[:2]] == ["1.50", "2.00"]


def test_non_strict_cast_turns_garbage_into_null():
    df = pl.DataFrame({"v": ["1.0", "abc"]})
    out = df.select(
        CastToNumbers("v").to_numbers(Numbers.FLOAT64, is_strict=False)
    ).to_series()
    assert out.to_list() == [1.0, None]


def test_strict_cast_raises_on_garbage():
    df = pl.DataFrame({"v": ["abc"]})
    with pytest.raises(pl.exceptions.PolarsError):
        df.select(CastToNumbers("v").to_numbers(Numbers.FLOAT64, is_strict=True))


def test_to_currency_prefixes_dollar_and_keeps_two_decimals():
    df = pl.DataFrame({"v": ["12.5", "", "3"]})
    out = df.select(CastToNumbers("v").to_currency()).to_series()
    assert out.name == "v"
    assert out.to_list() == ["$12.50", "$0.00", "$3.00"]
