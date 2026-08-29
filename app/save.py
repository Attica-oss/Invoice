"""Save dataframes to CSV files"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from all_dataframes.all_dataframes import (
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


# CSV writes are I/O-bound (Google Sheet fetch + parse in scan-google-sheet
# release the GIL, so does the Polars collect), so a thread pool gives a
# near-linear speed-up over saving one frame at a time.
_MAX_WORKERS = min(8, (os.cpu_count() or 4) * 2)


def save_to_csv(
    name: str, lf: pl.LazyFrame, output_dir: Path = OUTPUT_DIR
) -> SaveResult:
    """Save a LazyFrame to CSV and return a result object."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{name}.csv"

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
) -> tuple[list[str], list[tuple[str, Exception]]]:
    """Write every frame concurrently; collect successes and failures."""
    successes: list[str] = []
    failures: list[tuple[str, Exception]] = []

    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        futures = {
            pool.submit(save_to_csv, name, lf): name for name, lf in frames.items()
        }
        for future in as_completed(futures):
            result = future.result()
            if result.error:
                failures.append((result.name, result.error))
            else:
                successes.append(result.name)

    return successes, failures


def _save_category(
    category_name: str, category_dfs: dict[str, pl.LazyFrame]
) -> tuple[list[str], list[tuple[str, Exception]]]:
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
        all_successes, all_failures = _save_many(every_frame)

        logger.info(
            "Save completed. Success: %d, Failed: %d",
            len(all_successes),
            len(all_failures),
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

    successes, failures = _save_category(category, df_dict[category])

    logger.info(
        "Save completed for %s. Success: %d, Failed: %d",
        category,
        len(successes),
        len(failures),
    )
    if failures:
        logger.error(
            "Failed: %s",
            ", ".join(f"{name}: {err!s}" for name, err in failures),
        )
