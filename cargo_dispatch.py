import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    import polars as pl
    from dataframe import dispatch_to_cargo


@app.cell
def _():
    dispatch_to_cargo
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
