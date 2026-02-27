"""Saves LazyFrames to CSV files.
This is the main entry point for saving datasets to disk.
 It uses the registry of dataset builders to generate the data and save it in a structured way."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import polars as pl

from app.logger import logger
from all_dataframes.registry import REGISTRY, DatasetBuilder


DEFAULT_OUTPUT_DIR = Path.home() / "Invoice" / "csv"


@dataclass
class SaveOutcome:
    """Save outcome for a single dataset save operation."""

    category: str
    name: str
    path: Path
    error: Exception | None = None


def _write_lazyframe(lf: pl.LazyFrame, path: Path) -> None:
    """
    Docstring for _write_lazyframe

    :param lf: Description
    :type lf: pl.LazyFrame
    :param path: Description
    :type path: Path
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if hasattr(lf, "sink_csv"):
        lf.sink_csv(path)
    else:
        lf.collect().write_csv(path)


def save_one(
    category: str, name: str, build: DatasetBuilder, base_dir: Path
) -> SaveOutcome:
    """
    Docstring for save_one

    :param category: Description
    :type category: str
    :param name: Description
    :type name: str
    :param build: Description
    :type build: DatasetBuilder
    :param base_dir: Description
    :type base_dir: Path
    :return: Description
    :rtype: SaveOutcome
    """
    path = base_dir / category / f"{name}.csv"
    try:
        result = build()
        if result.is_err():
            err = result.unwrap_err()
            logger.error("Build failed %s/%s: %s", category, name, err)
            return SaveOutcome(category, name, path, error=err)

        lf = result.unwrap()
        _write_lazyframe(lf, path)
        logger.info("Saved %s/%s -> %s", category, name, path)
        return SaveOutcome(category, name, path)

    except Exception as e:
        logger.exception("Unexpected save failure %s/%s", category, name)
        return SaveOutcome(category, name, path, error=e)


def save_df_to_csv(
    category: str | None, output_dir: Path = DEFAULT_OUTPUT_DIR
) -> list[SaveOutcome]:
    """
    Docstring for save_df_to_csv

    :param category: Description
    :type category: str | None
    :param output_dir: Description
    :type output_dir: Path
    :return: Description
    :rtype: list[SaveOutcome]
    """
    if category is None:
        logger.error("No category provided")
        return []

    if category == "all":
        outcomes: list[SaveOutcome] = []
        for cat, builders in REGISTRY.items():
            for name, build in builders.items():
                outcomes.append(save_one(cat, name, build, output_dir))
        _print_summary(outcomes)
        return outcomes

    builders = REGISTRY.get(category)
    if not builders:
        logger.error(
            "Invalid category '%s'. Options: %s",
            category,
            list(REGISTRY.keys()) + ["all"],
        )
        return []

    outcomes = [
        save_one(category, name, build, output_dir) for name, build in builders.items()
    ]
    _print_summary(outcomes)
    return outcomes


def _print_summary(outcomes: list[SaveOutcome]) -> None:
    """Prints a summary of save outcomes, including counts of successes and failures, and details of any failures."""
    ok = [o for o in outcomes if o.error is None]
    bad = [o for o in outcomes if o.error is not None]

    print("\nSave summary")
    print("-----------")
    print(f"Success: {len(ok)}")
    print(f"Failed:  {len(bad)}")

    if bad:
        print("\nFailures:")
        for o in bad:
            print(f"- {o.category}/{o.name}: {o.error}")
    print()
