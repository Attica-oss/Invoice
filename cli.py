import marimo

__generated_with = "0.23.14"
app = marimo.App(width="columns")

with app.setup:
    import polars as pl
    import marimo as mo

    from app import app


@app.cell
def _():
    app.App().run()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
