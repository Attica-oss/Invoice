import marimo

__generated_with = "0.23.1"
app = marimo.App(width="columns")

with app.setup:
    import polars as pl
    import marimo as mo


@app.cell
def _():
    from dataframe import washing_lf

    return (washing_lf,)


@app.cell
def _(washing_lf):
    washing_lf.collect()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
