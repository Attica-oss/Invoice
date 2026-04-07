import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")

with app.setup:
    import polars as pl

    from dataframe.emr import washing


@app.cell
def _():
    washing.collect()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
