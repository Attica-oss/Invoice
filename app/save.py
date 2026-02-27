"""Save dataframes to CSV files"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


import polars as pl
from app.logger import logger


from all_dataframes.all_dataframes import (
    emr_dataframes,
    netlist_dataframes,
    bin_dispatch_dataframes,
    shore_handling_dataframes,
    miscellaneous_dataframes,
    operations_dataframes,
    stuffing_dataframes,
    transport_dataframes,
)


df_dict = {
    "emr": emr_dataframes,
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
            lf.collect(streaming=True).write_csv(path)

        logger.info("Wrote %s -> %s", name, path)
        return SaveResult(name=name, path=path)

    except (PermissionError, OSError, FileExistsError, FileNotFoundError) as e:
        logger.exception("Failed writing %s -> %s", name, path)
        return SaveResult(name=name, path=path, error=e)


def _save_category(
    category_name: str, category_dfs: dict[str, pl.LazyFrame]
) -> tuple[list[str], list[tuple[str, Exception]]]:
    logger.info("Processing category: %s", category_name)

    successes: list[str] = []
    failures: list[tuple[str, Exception]] = []

    for name, lf in category_dfs.items():
        result = save_to_csv(name, lf)
        if result.error:
            failures.append((result.name, result.error))
        else:
            successes.append(result.name)

    return successes, failures


def save_df_to_csv(category: str | None) -> None:
    """
    Save dataframes by category name or 'all'.
    """
    if category == "all":
        all_successes: list[str] = []
        all_failures: list[tuple[str, Exception]] = []

        for cat_name, cat_dfs in df_dict.items():
            s, f = _save_category(cat_name, cat_dfs)
            all_successes.extend(s)
            all_failures.extend(f)

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
