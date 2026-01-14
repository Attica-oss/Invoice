"""Save dataframes to CSV files"""

from pathlib import Path
from functools import cache


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


def save_to_csv(
    dataframe_info: tuple[str, pl.LazyFrame],
) -> tuple[str, Exception | None]:
    """Process the dataframes to CSV file
    Args:
        dataframe_info: Tuple containing the dataframe name and dataframe
    Returns:
        tuple: Tuple containing the dataframe name and error if any
    """

    if not isinstance(dataframe_info, tuple) or len(dataframe_info) != 2:
        return "Invalid data format", TypeError("Invalid data format")

    dataframe_name, dataframe = dataframe_info

    try:
        # Add explicit path and ensure directory exists
        # output_path = (
        #     rf"P:\Verification & Invoicing\Validation Report\csv\{dataframe_name}.csv"
        # )
        output_path = rf"{Path.home()}\Invoice\csv\{dataframe_name}.csv"
        dataframe.collect().write_csv(output_path)
        logger.info("Successfully wrote %s to file", dataframe_name)
        return dataframe_name, None
    except (FileExistsError, FileNotFoundError) as e:
        error_msg = f"Error writing {dataframe_name}: {str(e)}"
        logger.error(error_msg)
        return dataframe_name, e


@cache
def save_df_to_csv(dataframes: str | None):
    """
    Save the dataframes

    Args:
        dataframes: Category of dataframes to save ('all' or specific category name)

    """

    df_dict = {
        "emr": emr_dataframes,
        "operations": operations_dataframes,
        "netlist": netlist_dataframes,
        "bin_dispatch": bin_dispatch_dataframes,
        "shore_handling": shore_handling_dataframes,
        "stuffing": stuffing_dataframes,
        "transport": transport_dataframes,
        "miscellaneous": miscellaneous_dataframes,
        # "all": all_dataframes,
    }

    if dataframes == "all":
        # Process all dictionaries
        logger.info("Processing all dataframe categories")
        for category, category_dfs in df_dict.items():
            logger.info("Processing category: %s", category)
            for name, df in category_dfs.items():
                try:
                    result = save_to_csv((name, df))
                    if result is None:
                        logger.error("No result returned for %s", name)
                        continue

                    message, error = result
                    if error:
                        logger.error("Error saving %s: %s", message, str(error))
                    else:
                        logger.info("Successfully saved %s", message)
                except (ValueError, IOError) as e:
                    logger.error("Unexpected error processing %s: %s", name, str(e))

        logger.info("Completed processing all categories")
        return

    # Handle single category case
    data = df_dict.get(dataframes)

    print(data)
    if data is None:
        logger.error("Invalid dataframe option: %s", dataframes)
        return

    logger.info("Processing dataframe category: %s", dataframes)
    logger.info("Processing dataframes: %s", list(data.keys()))

    # Process each dataframe
    successes = []
    failures = []

    for name, df in data.items():
        try:
            result = save_to_csv((name, df))
            if result is None:
                logger.error("No result returned for %s", name)
                continue

            message, error = result
            if error:
                failures.append((message, error))
                logger.error("Error saving %s: %s", message, str(error))
            else:
                successes.append(message)
                logger.info("Successfully saved %s", message)
        except (IOError, ValueError) as e:
            logger.error("Unexpected error processing %s: %s", name, str(e))
            failures.append((name, e))

    # Summary
    logger.info("Save completed")
    logger.info("Successfully saved: %s", ", ".join(successes))
    if failures:
        logger.error(
            "Failed to save: %s",
            ", ".join(f"{name}: {str(err)}" for name, err in failures),
        )
