import marimo

__generated_with = "0.16.0"
app = marimo.App(width="medium")


@app.cell
def _():
    from dataframe.netlist import iot_cargo
    return (iot_cargo,)


@app.cell
def _(iot_cargo, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM iot_cargo
        """
    )
    return


@app.cell
def _():
    from dataframe.miscellaneous import dispatch_to_cargo
    return (dispatch_to_cargo,)


@app.cell
def _(dispatch_to_cargo, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM dispatch_to_cargo 
        """
    )
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


if __name__ == "__main__":
    app.run()
