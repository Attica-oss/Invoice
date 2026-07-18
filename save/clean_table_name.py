"""Helper functions for cleaning Excel table names."""

import re


def clean_table_name(name: str, index: int) -> str:
    """
    Create a valid, workbook-unique Excel table name.

    Excel table names:
    - cannot contain spaces
    - should start with a letter or underscore
    - must be unique within the workbook
    """
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", name)

    if not cleaned or cleaned[0].isdigit():
        cleaned = f"Table_{cleaned}"

    return f"{cleaned}_{index}"
