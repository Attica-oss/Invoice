import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells
    import marimo as mo
    import polars as pl


@app.cell
def _():
    dry_150 = 87.632
    brine_150 = 80.404
    dry_200 = 19.456
    brine_200= 42.743
    return


@app.cell
def _():
    # dry_150 + brine_150 + brine_200 + dry_200



    return


@app.cell
def _():
    sum([18.504,
    69.128,
    80.404,
    0,
    42.743,
    19.456])
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
