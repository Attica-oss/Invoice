import marimo

__generated_with = "0.18.3"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells
    import marimo as mo
    import polars as pl


@app.cell
def _():
    from app import app
    return (app,)


@app.cell
def _(app):
    app.App().run()
    return


@app.cell
def _():
    from type_casting import containers
    return (containers,)


@app.cell
def _(containers):
    containers.containers.collect()
    return


@app.cell
def _(containers):
    containers.container_list
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
