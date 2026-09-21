import marimo

__generated_with = "0.24.0"
app = marimo.App(width="full")

with app.setup:

    import marimo as mo
    import polars as pl
    # from dataframe.pre_trip_inspection import pre_trip_inspection
    from dataframe.emr import _pti as pti_before
    from datasets.emr import pti


@app.cell
def _():
    p_before = pti_before()
    return (p_before,)


@app.cell
def _():
    p_after = pti()
    return (p_after,)


@app.cell
def _(p_before):
    p_before
    return


@app.cell
def _(p_before):
    import polars.selectors as cs

    df = p_before.with_columns(cs.by_dtype(pl.Int128, pl.Decimal).cast(pl.Float64))
    return (df,)


@app.cell(hide_code=True)
def _(df, p_after):
    _df = mo.sql(
        f"""
        WITH
            a AS (
                FROM
                    p_after
            ),
            b AS (
                FROM
                    df
            )
            SELECT
            	SUM(a.total_price) AS a,
            	SUM(b.total_price) AS b,
            a.*,
            b.*,
            a.total_price::DECIMAL = b.total_price::DECIMAL AS _check
    
        FROM
            a
            LEFT JOIN b ON b.datetime_start = a.datetime_start
            AND a.container_number = b.container_number
        GROUP BY ALL

    
        """
    )
    return


@app.cell(hide_code=True)
def _():
    _df = mo.sql(
        f"""
        FROM READ_CSV('./csv/pti.csv')
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
