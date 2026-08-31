"""Raw sheet loads for the net-list pipelines, cached per process."""

from __future__ import annotations

from functools import lru_cache

import polars as pl

from data_source.make_dataset import load_sheet as scan_google_sheet
from data_source.sheet_ids import (
    NET_LIST_SHEET_NAME,
    OPS_SHEET_ID,
    RAW_DATA_SHEET_NAME,
)


@lru_cache(maxsize=1)
def net_list_raw() -> pl.LazyFrame:
    """The net-list operations sheet, unfiltered."""
    return scan_google_sheet(sheet_id=OPS_SHEET_ID, sheet_name=NET_LIST_SHEET_NAME)


@lru_cache(maxsize=1)
def genesis_raw() -> pl.LazyFrame:
    """The Genesis raw-data sheet, unfiltered."""
    return scan_google_sheet(sheet_id=OPS_SHEET_ID, sheet_name=RAW_DATA_SHEET_NAME)
