import marimo

__generated_with = "0.19.4"
app = marimo.App(width="medium")

with app.setup:
    import polars as pl
    import marimo as mo


@app.cell
def _():
    import sqlalchemy

    DATABASE_URL = r"sqlite:///C:\Users\gmounac\Desktop\operations\operations.db"
    engine = sqlalchemy.create_engine(DATABASE_URL)
    return (engine,)


@app.cell
def _(engine, net_list):
    all_df = mo.sql(
        f"""
        SELECT * FROM net_list
        """,
        engine=engine
    )
    return (all_df,)


@app.cell
def _(all_df):
    _df = mo.sql(
        f"""
        SELECT cast(date AS DATE) FROM all_df
        """
    )
    return


@app.cell
def _():
    from dataframe.netlist import oss,netList,stuffing_type
    return oss, stuffing_type


@app.cell
def _(oss):
    oss.collect()
    return


@app.cell
def _(oss):
    _df = mo.sql(
        f"""
        SELECT * FROM oss WHERE date = '2026-01-20'
        """
    )
    return


@app.cell
def _(netlist):
    _df = mo.sql(
        f"""
        SELECT * FROM netList WHERE destination = 'Asian Marine Reefer'
        """
    )
    return


@app.cell
def _(stuffing_type):
    _df = mo.sql(
        f"""
        SELECT * FROM stuffing_type
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
