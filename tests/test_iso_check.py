"""Tests for the ISO 6346 container-number validator.

Fixtures use two independently check-digit-computed numbers:
- CSQU3054383 - the ISO 6346 / Wikipedia worked example (check digit 3)
- TGHU1234567 - hand-computed: weighted sum 5529, 5529 % 11 == 7
"""

from __future__ import annotations

import pytest

from type_casting.iso_check import ContainerValidator

VALID = ["CSQU3054383", "TGHU1234567"]
BAD_CHECK_DIGIT = ["CSQU3054380", "TGHU1234560"]


@pytest.fixture
def validator() -> ContainerValidator:
    return ContainerValidator()


@pytest.mark.parametrize("number", VALID)
def test_valid_numbers_pass(validator, number):
    assert validator.validate_check_digit(number) is True
    assert validator.validate_container_number(number) is True


@pytest.mark.parametrize("number", BAD_CHECK_DIGIT)
def test_wrong_check_digit_fails(validator, number):
    assert validator.validate_check_digit(number) is False
    assert validator.validate_container_number(number) is False


@pytest.mark.parametrize(
    "number",
    [
        "tghu1234567",   # lowercase owner code
        "TGHU123456",    # 10 chars - too short
        "TGHU12345678",  # 12 chars - too long
        "TGH11234567",   # digit in the owner-code region
        "",
    ],
)
def test_malformed_numbers_are_rejected(validator, number):
    assert validator.validate_container_number(number) is False


def test_read_input_strips_whitespace(validator):
    assert validator.read_input([" CSQU3054383 ", "TGHU1234567 "]) == VALID
