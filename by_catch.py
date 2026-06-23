import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")

with app.setup:
    import polars as pl
    import marimo as mo
    from dataframe import by_catch


@app.cell
def _():
    by_catch
    return


@app.cell
def _():
    from dataframe.miscellaneous import BY_CATCH_PRICE

    return (BY_CATCH_PRICE,)


@app.cell
def _(BY_CATCH_PRICE):
    BY_CATCH_PRICE
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
