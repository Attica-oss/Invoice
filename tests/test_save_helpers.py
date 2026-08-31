"""Tests for the Excel-export naming helpers in the save package."""

from __future__ import annotations

from save.clean_table_name import clean_table_name
from save.export_dataframe import _unique_sheet_name


class TestCleanTableName:
    def test_spaces_and_punctuation_become_underscores(self):
        assert clean_table_name("Net List", 2) == "Net_List_2"
        assert clean_table_name("a-b/c.d", 3) == "a_b_c_d_3"

    def test_leading_digit_gets_table_prefix(self):
        assert clean_table_name("2024 data", 1) == "Table_2024_data_1"

    def test_empty_name_gets_table_prefix(self):
        assert clean_table_name("", 0) == "Table__0"

    def test_index_is_always_suffixed(self):
        assert clean_table_name("sheet", 7).endswith("_7")


class TestUniqueSheetName:
    def test_returns_name_unchanged_when_free(self):
        assert _unique_sheet_name("Summary", set()) == "Summary"

    def test_deduplicates_case_insensitively_with_counter(self):
        used: set[str] = set()
        assert _unique_sheet_name("Sheet", used) == "Sheet"
        assert _unique_sheet_name("Sheet", used) == "Sheet (2)"
        assert _unique_sheet_name("sheet", used) == "sheet (3)"

    def test_truncates_to_excel_31_char_limit(self):
        long = "N" * 40
        assert _unique_sheet_name(long, set()) == "N" * 31

    def test_truncates_before_appending_dedupe_suffix(self):
        long = "N" * 40
        used: set[str] = set()
        first = _unique_sheet_name(long, used)
        second = _unique_sheet_name(long, used)
        assert first == "N" * 31
        assert len(second) <= 31
        assert second.endswith(" (2)")
