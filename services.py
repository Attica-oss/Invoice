import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")

with app.setup:
    import polars as pl
    import marimo as mo
    from dataframe.shore_handling import forklift_salt
    from dataframe.transport import transfer


@app.cell
def _():
    fs = forklift_salt().collect()
    return (fs,)


@app.cell
def _():
    transfer.collect()
    return


@app.cell(hide_code=True)
def _(forklift):
    normal_salt = mo.sql(
        f"""
        FROM
            forklift
        SELECT
            * EXCLUDE (overtime_150, overtime_200, normal_hours,duration),
            CEIL(
                (
                    EXTRACT (
                        HOUR
                        FROM
                            duration
                    ) + EXTRACT (
                        MINUTE
                        FROM
                            duration
                    ) / 60.0 + EXTRACT (
                        SECOND
                        FROM
                            duration
                    ) / 3600.0
                )
            ) AS "hours",
            CEIL(overtime_150 / 60) * 45 + CEIL(overtime_200 / 60) * 60 + CEIL(normal_hours / 60) * 30 AS total_price
        """
    )
    return (normal_salt,)


@app.cell
def _(fs):
    forklift_salty = mo.sql(
        f"""
        WITH forklift_ AS (FROM
            fs
        SELECT
            * EXCLUDE (
                overtime_for_normal_services,
                overtime_for_extended_services,
                normal_hour_services
            ),
            CEIL(
                (
                    EXTRACT (
                        HOUR
                        FROM
                            normal_hour_services
                    ) + EXTRACT (
                        MINUTE
                        FROM
                            normal_hour_services
                    ) / 60.0 + EXTRACT (
                        SECOND
                        FROM
                            normal_hour_services
                    ) / 3600.0
                )
            ) * 30 AS normal_hour_services,
            CEIL(
                (
                    EXTRACT (
                        HOUR
                        FROM
                            overtime_for_normal_services
                    ) + EXTRACT (
                        MINUTE
                        FROM
                            overtime_for_normal_services
                    ) / 60.0 + EXTRACT (
                        SECOND
                        FROM
                            overtime_for_normal_services
                    ) / 3600.0
                )
            ) * 45 AS overtime_for_normal_services,
            CEIL(
                (
                    EXTRACT (
                        HOUR
                        FROM
                            overtime_for_extended_services
                    ) + EXTRACT (
                        MINUTE
                        FROM
                            overtime_for_extended_services
                    ) / 60.0 + EXTRACT (
                        SECOND
                        FROM
                            overtime_for_extended_services
                    ) / 3600.0
                )
            ) * 60 AS overtime_for_extended_services)

        FROM forklift_
        SELECT date,vessel,CEIL(
                (
                    EXTRACT (
                        HOUR
                        FROM
                            total_duration
                    ) + EXTRACT (
                        MINUTE
                        FROM
                            total_duration
                    ) / 60.0 + EXTRACT (
                        SECOND
                        FROM
                            total_duration
                    ) / 3600.0
                )
            ) AS "hours",SUM(normal_hour_services + overtime_for_normal_services + overtime_for_extended_services) AS total_price
        GROUP BY ALL
        HAVING YEAR(date) = 2026
        ORDER BY date
        """
    )
    return


@app.cell
def _():
    from scan_google_sheet import scan_google_sheet

    return (scan_google_sheet,)


@app.cell
def _(scan_google_sheet):
    via_skiff = scan_google_sheet(
        url="https://docs.google.com/spreadsheets/d/1VbfiiWsp8yxs6KSR1CXpw1S_35tYlWV8UjjWah9Afpw/edit?pli=1&gid=61862422#gid=61862422",
        sheet_name="IPHSTruck",
    )
    return (via_skiff,)


@app.cell(hide_code=True)
def _(via_skiff):
    _df = mo.sql(
        f"""
        FROM via_skiff 
        SELECT *,CASE WHEN day IN ('Sun','PH') THEN total_tonnage * 11 * 1.5 ELSE total_tonnage * 11 END AS price
        """
    )
    return


@app.cell
def _():
    430.22099999999995
    360.558
    return


@app.cell(hide_code=True)
def _():
    _df = mo.sql(
        f"""
        FROM transfer
        SELECT month(date) AS month_number,MONTHNAME(date) AS month_name,remarks,COUNT(*),SUM(shifting_price),SUM(haulage_price)
            WHERE status = 'Empty'
        GROUP BY ALL
        ORDER BY month_number
        """
    )
    return


@app.cell(hide_code=True)
def _(normal_salt):
    _df = mo.sql(
        f"""
        FROM normal_salt
        SELECT month(date) AS month_number,MONTHNAME(date) AS month_name,customer,SUM("hours") AS tons,SUM(total_price)
        WHERE YEAR(date) = 2026
        GROUP BY ALL
        ORDER BY month_number
        """
    )
    return


@app.cell
def _():
    52 * 30
    return


@app.cell
def _():
    from dataframe.operations import berth

    return (berth,)


@app.cell(hide_code=True)
def _(berth):
    _df = mo.sql(
        f"""
        FROM berth
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
