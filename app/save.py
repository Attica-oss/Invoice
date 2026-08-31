"""Save dataframes to CSV files"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from all_dataframes.all_dataframes import (
    EXCEL_BACKED_NAMES,
    bin_dispatch_dataframes,
    emr_dataframes,
    invoice_dataframes,
    miscellaneous_dataframes,
    netlist_dataframes,
    operations_dataframes,
    shore_handling_dataframes,
    stuffing_dataframes,
    transport_dataframes,
    washing_dataframe,
)
from app.logger import logger
from data_source.make_dataset import is_windows

df_dict = {
    "emr": emr_dataframes,
    "invoice": invoice_dataframes,
    "washing": washing_dataframe,
    "operations": operations_dataframes,
    "netlist": netlist_dataframes,
    "bin_dispatch": bin_dispatch_dataframes,
    "shore_handling": shore_handling_dataframes,
    "stuffing": stuffing_dataframes,
    "transport": transport_dataframes,
    "miscellaneous": miscellaneous_dataframes,
}

# Save Location
OUTPUT_DIR = Path.home() / "Invoice" / "csv"


@dataclass(frozen=True)
class SaveResult:
    """Data class to represent the result of a save operation"""

    name: str
    path: Path
    error: Exception | None = None
    skip_reason: str | None = None


# CSV writes are I/O-bound (Google Sheet fetch + parse in scan-google-sheet
# release the GIL, so does the Polars collect), so a thread pool gives a
# near-linear speed-up over saving one frame at a time.
_MAX_WORKERS = min(8, (os.cpu_count() or 4) * 2)

_WINDOWS_ONLY_EMPTY_SCHEMA = "Excel-backed source, unavailable on this OS (Windows-only)"


def save_to_csv(
    name: str, lf: pl.LazyFrame, output_dir: Path = OUTPUT_DIR
) -> SaveResult:
    """Save a LazyFrame to CSV and return a result object."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{name}.csv"

    # dataframe.operations's Excel readers return an empty, zero-column
    # LazyFrame on non-Windows -- for the names known to depend on them,
    # skip here instead of letting a downstream select() on a missing
    # column surface as a generic write failure.
    if name in EXCEL_BACKED_NAMES and not is_windows():
        logger.info("Skipped %s: %s", name, _WINDOWS_ONLY_EMPTY_SCHEMA)
        return SaveResult(name=name, path=path, skip_reason=_WINDOWS_ONLY_EMPTY_SCHEMA)

    try:
        # Prefer streaming write if available
        if hasattr(lf, "sink_csv"):
            lf.sink_csv(path)  # Polars streaming write
        else:
            lf.collect(engine="streaming").write_csv(path)

        logger.info("Wrote %s -> %s", name, path)
        return SaveResult(name=name, path=path)

    except Exception as e:  # noqa: BLE001 - one bad frame must not abort the batch
        logger.exception("Failed writing %s -> %s", name, path)
        return SaveResult(name=name, path=path, error=e)


def _save_many(
    frames: dict[str, pl.LazyFrame],
) -> tuple[list[str], list[tuple[str, Exception]], list[tuple[str, str]]]:
    """Write every frame concurrently; collect successes, failures, and skips."""
    successes: list[str] = []
    failures: list[tuple[str, Exception]] = []
    skipped: list[tuple[str, str]] = []

    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        futures = {
            pool.submit(save_to_csv, name, lf): name for name, lf in frames.items()
        }
        for future in as_completed(futures):
            result = future.result()
            if result.skip_reason:
                skipped.append((result.name, result.skip_reason))
            elif result.error:
                failures.append((result.name, result.error))
            else:
                successes.append(result.name)

    return successes, failures, skipped


def _save_category(
    category_name: str, category_dfs: dict[str, pl.LazyFrame]
) -> tuple[list[str], list[tuple[str, Exception]], list[tuple[str, str]]]:
    logger.info("Processing category: %s", category_name)
    return _save_many(category_dfs)


def save_df_to_csv(category: str | None) -> None:
    """
    Save dataframes by category name or 'all'.
    """
    if category == "all":
        # One pool across every category so all sheet fetches overlap
        # instead of running category by category.
        every_frame = {
            name: lf for cat_dfs in df_dict.values() for name, lf in cat_dfs.items()
        }
        all_successes, all_failures, all_skipped = _save_many(every_frame)

        logger.info(
            "Save completed. Success: %d, Failed: %d, Skipped: %d",
            len(all_successes),
            len(all_failures),
            len(all_skipped),
        )
        if all_skipped:
            logger.info(
                "Skipped (expected): %s",
                ", ".join(f"{name}: {reason}" for name, reason in all_skipped),
            )
        if all_failures:
            logger.error(
                "Failed: %s",
                ", ".join(f"{name}: {err!s}" for name, err in all_failures),
            )
        return

    if not category or category not in df_dict:
        logger.error(
            "Invalid dataframe option: %s. Options: %s",
            category,
            list(df_dict.keys()) + ["all"],
        )
        return

    successes, failures, skipped = _save_category(category, df_dict[category])

    logger.info(
        "Save completed for %s. Success: %d, Failed: %d, Skipped: %d",
        category,
        len(successes),
        len(failures),
        len(skipped),
    )
    if skipped:
        logger.info(
            "Skipped (expected): %s",
            ", ".join(f"{name}: {reason}" for name, reason in skipped),
        )
    if failures:
        logger.error(
            "Failed: %s",
            ", ".join(f"{name}: {err!s}" for name, err in failures),
        )
