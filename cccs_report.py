import polars as pl
from dataframe.miscellaneous import by_catch, cccs_container_stuffing


def get_month_and_date() -> tuple[int, int]:
    """
    Extracts month and date from the 'date' column in the DataFrame.

    Args:
        df (pl.DataFrame): Input DataFrame with a 'date' column.

    Returns:
        pl.DataFrame: DataFrame with 'month' and 'date' columns.
    """

    get_month = int(input("Enter month (1-12): "))
    get_date = int(input("Enter date (1-31): "))
    return get_month, get_date


def by_catch_df() -> pl.DataFrame:
    """By catch dataframe."""
    return (
        by_catch.group_by(pl.col("date"), pl.col("customer"), maintain_order=True)
        .agg(
            pl.col("total_tonnage").sum().alias("total_tonnage"),
            pl.col("overtime_tonnage").sum().alias("overtime_tonnage"),
        )
        .with_columns(normal=pl.col("total_tonnage") - pl.col("overtime_tonnage"))
        .collect()
    )


def cccs_container_stuffing_df() -> pl.DataFrame:
    """CCCS container stuffing dataframe."""
    return (
        cccs_container_stuffing().group_by(
            pl.col("date"), pl.col("customer"), maintain_order=True
        )
        .agg(
            pl.col("total_tonnage").sum().alias("total_tonnage"),
            pl.col("overtime_tonnage").sum().alias("overtime_tonnage"),
        )
        .with_columns(normal=pl.col("total_tonnage") - pl.col("overtime_tonnage"))
        .collect()
    )


def filter_dataframe(df: pl.DataFrame,month:int,date:int) -> pl.DataFrame:
    """
    Filters the DataFrame based on the specified month and date.

    Args:
        df (pl.DataFrame): Input DataFrame to filter.
        month (int): Month to filter by.
        date (int): Date to filter by.

    Returns:
        pl.DataFrame: Filtered DataFrame.
    """
    # month, date = get_month_and_date()
    # if not (1 <= month <= 12) or not (1 <= date <= 31):
    #     raise ValueError("Invalid month or date provided.")
    return df.filter(
        pl.col("date").dt.month().eq(month).and_(pl.col("date").dt.day().eq(date))
    )


def print_results(df: pl.DataFrame) -> None:
    """Prints the results of the DataFrame."""
    print("Results:")
    print(df)
    print()
    print(
        df.select(
            pl.col("total_tonnage").sum().round(3).alias("total_tonnage")
        ).to_dict(as_series=False)
    )

    print(
        df.select(
            pl.col("normal").sum().round(3).alias("total_normal_tonnage")
        ).to_dict(as_series=False)
    )
    print(
        df.select(
            pl.col("overtime_tonnage").sum().round(3).alias("total_overtime_tonnage")
        ).to_dict(as_series=False)
    )


def main() -> str:
    """main function to generate the CC/CS report."""
    month, date = get_month_and_date()
    print("Generating CCCS report...")
    print(f"Data for : {date}/{month}/2025")
    print("\nBYCATCH DATA:")
    print_results(filter_dataframe(by_catch_df(),month=month,date=date))
    print("\nCCCS CONTAINER STUFFING DATA")
    print_results(filter_dataframe(cccs_container_stuffing_df(),month=month,date=date))


if __name__ == "__main__":
    main()
    # Uncomment the following lines to run the functions
    # df = pl.read_csv("data.csv")  # Example DataFrame loading
    # by_catch(df)
    # cccs_container_stuffing(df)
    # get_month_and_date()  # Call to get month and date
