"""Tests for type_casting.validations - overtime billing expressions and enums."""

from __future__ import annotations

import polars as pl
import pytest

from type_casting.validations import (
    MovementType,
    Overtime,
    OvertimePerc,
    PalletType,
    SetPoint,
    apply_overtime_rate,
    overtime_rate_multiplier,
    unknown_overtime_rows,
)

_LABELS = pl.DataFrame(
    {
        "overtime": [
            Overtime.normal_hour_text,
            Overtime.overtime_150_text,
            Overtime.overtime_200_text,
            "something else",
        ],
        "unit_price": [10.0, 10.0, 10.0, 10.0],
    }
)


def test_overtime_rate_multiplier_maps_known_bands():
    out = _LABELS.select(overtime_rate_multiplier()).to_series().to_list()
    assert out == [
        OvertimePerc.normal_hour,
        OvertimePerc.overtime_150,
        OvertimePerc.overtime_200,
        None,
    ]


def test_overtime_rate_multiplier_unknown_label_is_null_not_a_default():
    assert _LABELS.select(overtime_rate_multiplier()).to_series().to_list()[-1] is None


def test_apply_overtime_rate_multiplies_price_and_nulls_unknown():
    out = _LABELS.select(apply_overtime_rate()).to_series().to_list()
    assert out == [10.0, 15.0, 20.0, None]


def test_apply_overtime_rate_custom_column_names():
    df = pl.DataFrame({"ot": [Overtime.overtime_200_text], "rate": [7.0]})
    out = df.select(apply_overtime_rate(price_col="rate", overtime_col="ot"))
    assert out.to_series().to_list() == [14.0]


def test_unknown_overtime_rows_filters_only_unrecognized_labels():
    out = unknown_overtime_rows(_LABELS.lazy()).collect()
    assert out["overtime"].to_list() == ["something else"]


def test_validation_enum_values_are_stable():
    assert PalletType.LINER_PALLET == "Liner & Pallet"
    assert MovementType.delivery == "Delivery"
    assert SetPoint.magnum == "-35"


@pytest.mark.parametrize(
    ("attr", "expected"),
    [("normal_hour", 1.0), ("overtime_150", 1.5), ("overtime_200", 2.0)],
)
def test_overtime_percentages(attr, expected):
    assert getattr(OvertimePerc, attr) == expected
