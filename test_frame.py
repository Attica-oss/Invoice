import marimo

__generated_with = "0.24.0"
app = marimo.App(width="columns")

with app.setup:
    import polars as pl
    import marimo as mo

    from dataframe import bin_dispatch

    from type_casting import CastToNumbers,Numbers


@app.cell
def _():
    bin_dispatch.full_scows.with_columns(
        CastToNumbers(column_name="num_of_scows").to_numbers(to=Numbers.INT16),
        CastToNumbers(column_name="tonnage").to_numbers(to=Numbers.DECIMAL,scale=3),
        CastToNumbers(column_name="unit_price").to_currency(scale=2),
        CastToNumbers(column_name="total_price").to_currency(scale=3)
    ).collect()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
