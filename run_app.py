import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells
    import marimo as mo
    import polars as pl
    from app.app import App


@app.cell
def _():
    App().run()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
