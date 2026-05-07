import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")

with app.setup:
    import polars as pl
    import marimo as mo
    from dataframe import transfer


@app.cell
def _():
    transfer.collect()
    return


@app.cell(hide_code=True)
def _(netlist):
    _df = mo.sql(
        f"""
        FROM netList WHERE YEAR(date) = 2025
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
