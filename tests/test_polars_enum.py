"""Tests for the PolarsEnum StrEnum helper base."""

from __future__ import annotations

import polars as pl
import pytest

from type_casting.polars_enum import PolarsEnum


class Color(PolarsEnum):
    RED = "Red"
    BLUE = "Blue"


def test_list_all_preserves_declaration_order():
    assert Color.list_all() == ["Red", "Blue"]


def test_enum_dtype():
    assert Color.enum_dtype() == pl.Enum(["Red", "Blue"])


def test_has_value_is_exact():
    assert Color.has_value("Red")
    assert not Color.has_value("red")
    assert not Color.has_value("Green")


def test_parse_exact_or_raises():
    assert Color.parse("Red") is Color.RED
    with pytest.raises(ValueError, match="Invalid Color"):
        Color.parse("red")


def test_normalize_is_case_and_whitespace_tolerant():
    assert Color.normalize(" red ") == "Red"
    assert Color.normalize("BLUE") == "Blue"
    assert Color.normalize("Red") == "Red"


def test_normalize_rejects_none_and_unknown():
    with pytest.raises(ValueError):
        Color.normalize(None)
    with pytest.raises(ValueError, match="Invalid Color"):
        Color.normalize("green")


def test_lit_casts_normalized_value_to_enum():
    out = pl.select(Color.lit("red").alias("c"))
    assert out.schema["c"] == Color.enum_dtype()
    assert out["c"].to_list() == ["Red"]


def test_cast_col_accepts_name_or_expr():
    df = pl.DataFrame({"c": ["Red", "Blue"]})
    from_name = df.select(Color.cast_col("c"))
    from_expr = df.select(Color.cast_col(pl.col("c")))
    assert from_name.schema["c"] == Color.enum_dtype()
    assert from_expr.equals(from_name)
