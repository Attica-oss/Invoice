"""Enumeration of Excel file paths and their corresponding sheet names."""
from pathlib import Path
from enum import Enum

class ExcelFiles(Enum):
    """Enumeration of Excel file paths and their corresponding sheet names."""

    CONTAINER_OPERATIONS = (
        Path(
            rf"{Path.home()}/Dropbox/Container and Transport/Container Section/Container Operations Activity/Container Operation Activity.xlsx"
        ),
        "Plug in Plug out",
    )
    SALT_HANDLING = (
        Path(
            rf"{Path.home()}/Dropbox\Container and Transport\Salt Handling\IPHS Salt Operations.xlsx"
        ),
        "Salt Operations",
    )
    CONTAINER_TRANSFER = (
        Path(
            rf"{Path.home()}/Dropbox\Container and Transport\Transport Section\Container Movements\Container Transfer.xlsx"
        ),
        "Transfer",
    )
    SCOW_TRANSFER = (
        Path(
            rf"{Path.home()}/Dropbox\Container and Transport\Transport Section\Container Movements\STDU Transfer.xlsx"
        ),
        "STDU Transfer",
    )
    CONTAINER_SHIFTING = (
        Path(
            rf"{Path.home()}/Dropbox\Container and Transport\Transport Section\Container Shifting Records\IPHS Container Shifting Record.xlsx"
        ),
        "Container Shifting",
    )
    FORKLIFT_USAGE = (
        Path(
            rf"{Path.home()}/Dropbox\Container and Transport\Transport Section\Forklift Usage\Forklift Record.xlsx"
        ),
        "Forklift_Operation",
    )
    OPERATIONS_ACTIVITY = (
        Path(
            rf"{Path.home()}/Dropbox\! OPERATION SUPPORTING DOCUMENTATION\2025\2025 IPHS operation activity.xlsx"
        ),
        "HANDLING ACTIVITY",
    )

    OPERATIONS_ACTIVITY_2026 = (
        Path(
            rf"{Path.home()}/Dropbox\! OPERATION SUPPORTING DOCUMENTATION\2026\2026 IPHS operation activity.xlsx"
        ),
        "HANDLING ACTIVITY",
    )

    BERTH_DUES_2026 = (
        Path(
            rf"{Path.home()}/Dropbox\! OPERATION SUPPORTING DOCUMENTATION\2026\2026 IPHS operation activity.xlsx"
        ),
        "BERTH DUES IPHS",
    )

    ADDITIONAL_OVERTIME = (
        Path(
            rf"{Path.home()}/Dropbox\! OPERATION SUPPORTING DOCUMENTATION\2025\2025 IPHS operation activity.xlsx"
        ),
        "Overtime",
    )
    EXTRA_MEN = (
        Path(
            rf"{Path.home()}/Dropbox\! OPERATION SUPPORTING DOCUMENTATION\2025\2025 IPHS operation activity.xlsx"
        ),
        "Extramen",
    )
    BERTH_DUES = (
        Path(
            rf"{Path.home()}/Dropbox\! OPERATION SUPPORTING DOCUMENTATION\2025\2025 IPHS operation activity.xlsx"
        ),
        "BERTH DUES IPHS",
    )

    MOVEMENT_OUT = (
        "P:\Verification & Invoicing\Validation Report\Validation Report - 2024.xlsx","movement_out"
    )