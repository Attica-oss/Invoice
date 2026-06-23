import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def _():
    from dataframe import forklift

    return (forklift,)


@app.cell
def _():
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(forklift, mo):
    _df = mo.sql(
        f"""
        FROM forklift
        LIMIT 5
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
