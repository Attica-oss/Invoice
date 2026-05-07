import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")

with app.setup:
    import polars as pl
    import marimo as mo
    from dataframe import salt,forklift_for_salt,forklift
    from scan_google_sheet import scan_google_sheet


@app.cell(hide_code=True)
def _():
    _df = mo.sql(
        f"""
        FROM
            salt
        SELECT 
            SUM(normal) AS normal_tonnage,
            SUM(overtime_150) AS overtime_tonnage,
            SUM(overtime_200) AS overtime_2_tonnage
        WHERE
            customer LIKE '%HARTSWATER%'
            AND MONTH(date) = 1
        """
    )
    return


@app.cell(hide_code=True)
def _():
    _df = mo.sql(
        f"""
        FROM forklift_for_salt
    
        WHERE
            customer LIKE '%HARTSWATER%'
            AND MONTH(date) = 1
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
