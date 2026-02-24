import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")

with app.setup:
    import polars as pl
    import marimo as mo


@app.cell
def _():
    from dataframe.operations import tare

    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
