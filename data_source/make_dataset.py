"""Make the dataset (LazyFrame) from a google sheet id and sheet names.

Every Google Sheet read in the codebase goes through this module -- either
:func:`load_gsheet_data` (positional, the long-standing API) or
:func:`load_sheet` (keyword form, also a drop-in for ``scan_google_sheet``).
Results are memoised per process on ``(sheet_id, sheet_name, url, query,
parse_dates)`` so a given tab is fetched over the network at most once per
run, no matter how many pipelines ask for it.

``scan_google_sheet`` 0.3.0 downloads and parses the whole sheet once, up
front, then replays it from memory -- so caching the returned ``LazyFrame``
deduplicates both the HTTP round-trip and the CSV parse.
"""

from __future__ import annotations

from functools import lru_cache

import polars as pl
from scan_google_sheet import scan_google_sheet as _scan_google_sheet

from data_source.excel_file_path import ExcelFiles

_TIMEOUT = 20


@lru_cache(maxsize=None)
def _scan_cached(
    sheet_id: str | None,
    sheet_name: str,
    url: str | None,
    query: str | None,
    parse_dates: bool,
) -> pl.LazyFrame:
    """One network fetch per distinct argument tuple."""
    return _scan_google_sheet(
        sheet_name,
        sheet_id=sheet_id,
        url=url,
        query=query,
        parse_dates=parse_dates,
        timeout=_TIMEOUT,
    )


def load_sheet(
    sheet_name: str,
    sheet_id: str | None = None,
    url: str | None = None,
    *,
    query: str | None = None,
    parse_dates: bool = True,
    timeout: int | None = None,  # accepted for drop-in compatibility; unused
) -> pl.LazyFrame:
    """Load a Google Sheet tab as a cached Polars ``LazyFrame``.

    Signature-compatible with ``scan_google_sheet`` so existing call sites
    can switch with just an import alias. Provide exactly one of
    ``sheet_id`` or ``url``.

    ``query`` is an optional Google Visualization API Query Language string
    (columns referenced by spreadsheet letter, e.g.
    ``"select A, C, K where YEAR(K) = 2026"``). Google applies it before the
    download, so only matching rows/columns cross the wire -- use it to push
    year/type filters server-side instead of pulling whole history.
    """
    if (sheet_id is None) == (url is None):
        raise ValueError("Provide exactly one of sheet_id or url.")
    return _scan_cached(sheet_id, sheet_name, url, query, parse_dates)


def load_gsheet_data(
    sheet_id: str,
    sheet_name: str,
    query: str | None = None,
) -> pl.LazyFrame:
    """Load a Google Sheet as a cached Polars ``LazyFrame`` (positional API)."""
    return _scan_cached(sheet_id, sheet_name, None, query, True)


def clear_sheet_cache() -> None:
    """Drop every memoised sheet -- forces the next read to hit the network."""
    _scan_cached.cache_clear()


def load_excel(file_path: ExcelFiles) -> pl.LazyFrame:
    """Load an Excel file as a Polars ``LazyFrame``."""
    file, sheet = file_path.value
    return pl.read_excel(source=file, sheet_name=sheet).lazy()
