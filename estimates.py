import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")

with app.setup:
    import polars as pl
    import marimo as mo
    from datetime import date
    from dataframe.bin_dispatch import full_scows, empty_scows
    from dataframe.shore_handling import salt, forklift_salt
    from data_source.sheet_ids import INVOICING,REPORT_STATUS
    from scan_google_sheet import scan_google_sheet
    from dataframe.stuffing import coa


@app.cell
def _():
    invoicing_df = scan_google_sheet(sheet_id=INVOICING,sheet_name=REPORT_STATUS)
    return (invoicing_df,)


@app.cell
def _(invoicing_df):
    _df = mo.sql(
        f"""
        FROM invoicing_df
            SELECT
            "month" AS invoice_month,
            report_type,
            sub_type AS number,
            "vessel/client" AS vessel,
            customer,
            start_date,
            start_time,
            CASE
                WHEN end_date = '' THEN NULL
                ELSE STRPTIME(end_date, '%d/%m/%Y')::DATE
   
            END AS end_date,
                CASE
                WHEN end_time = '' THEN NULL
                ELSE STRPTIME(end_time, '%H:%M:%S')::TIME
   
            END AS end_time
        WHERE status <> 'Send for Approval'
        """
    )
    return


@app.cell(hide_code=True)
def _(invoicing_df):
    sto_report_df = mo.sql(
        f"""
        FROM
            invoicing_df
        SELECT
            sub_type AS number,
            "vessel/client" AS vessel,
            customer,
            start_date,
            CASE
                WHEN end_date = '' THEN NULL
                ELSE STRPTIME(end_date, '%d/%m/%Y')::DATE
            END AS end_date
        WHERE
            report_type = 'STO'
        """
    )
    return (sto_report_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Berth Dues
    """)
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # IOT - Bin Dispatch Monthly
    """)
    return


@app.cell(hide_code=True)
def _():
    full_scow_df = mo.sql(
        f"""
        -- Full Scows
        WITH
            scow_transfer AS (
                FROM
                    full_scows
                SELECT
                    MONTH(date) AS month_number,
                    MONTHNAME(date) AS month,

                    CAST(SUM(total_price) AS DECIMAL) AS total_price,
                    'MONTHLY' AS report_type,
                    'BIN DISPATCH TO IOT' AS sub_type,
            	'SCOW TRANSFER' AS service
                GROUP BY ALL
                ORDER BY
                    month_number
            ),
            forklift AS (
                FROM
                    full_scows
                SELECT
                    date,
                    overtime,
                    num_of_scows,
                    CASE
                        WHEN overtime = 'overtime 200%' THEN 2.0 * 6 * num_of_scows
                        WHEN overtime = 'overtime 150%' THEN 1.5 * 6 * num_of_scows
                        ELSE 1.0 * 6 * num_of_scows
                    END AS forklift_price
            ),forklift_grouped AS (
            FROM
            forklift
        SELECT
            MONTH(date) AS month_number,
            MONTHNAME(date) AS month,
            CAST(SUM(forklift_price) AS DECIMAL) AS total_price,
            'MONTHLY' AS report_type,
            'BIN DISPATCH TO IOT' AS sub_type,
            'FORKLIFT FOR SCOW HANDLING' AS service
        GROUP BY ALL
        ORDER BY
            month_number
            ),movement_fee  AS ( FROM
                    full_scows
                SELECT
                    MONTH(date) AS month_number,
                    MONTHNAME(date) AS month,

                    CAST(SUM(total_price) AS DECIMAL) AS total_price,
                    'MONTHLY' AS report_type,
                    'BIN DISPATCH TO IOT' AS sub_type,
            	'MOVEMENT FEE' AS service
                GROUP BY ALL
                ORDER BY
                    month_number),final AS (
        SELECT * FROM scow_transfer
        UNION
        SELECT * FROM forklift_grouped
        UNION
        SELECT * FROM movement_fee

        ORDER BY month_number

                    )
        FROM final
        SELECT month_number,month,SUM(total_price) AS total_price,'BIN DISPATCH TO IOT' AS service
        GROUP BY ALL
        ORDER BY month_number
        """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Salt Operation
    """)
    return


@app.cell
def _():
    forklift_salt_df = forklift_salt().filter(pl.col("date").dt.year().eq(2026))
    return (forklift_salt_df,)


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(forklift_salt_df, sto_report_df):
    forklift_raw_salt = mo.sql(
        f"""
        WITH forklift_ AS (FROM
            forklift_salt_df
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
            ) * 60 AS overtime_for_extended_services),sto AS (FROM sto_report_df),add_sto_number AS (

            FROM forklift_ f LEFT JOIN sto s ON f.date BETWEEN s.start_date AND s.end_date AND s.vessel = f.vessel

            )



        FROM add_sto_number
        SELECT 
        month(date) AS month_number,monthname(date) AS "month",vessel,customer,SUM(normal_hour_services + overtime_for_normal_services + overtime_for_extended_services) AS total_price,CASE WHEN customer LIKE '%HARTSWATER%' THEN 'HARTSWATER MONTHLY' WHEN customer LIKE '%ECHEBASTAR%' THEN 'ECHEBASTAR MONTHLY' ELSE number END AS report_type,'FORKLIFT SALT' AS service

        GROUP BY ALL
        ORDER BY month_number
        """
    )
    return (forklift_raw_salt,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## STO Salt
    """)
    return


@app.cell(hide_code=True)
def _(forklift_raw_salt):
    _df = mo.sql(
        f"""
        FROM forklift_raw_salt
        """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Monthly Salt
    """)
    return


@app.cell(hide_code=True)
def _(forklift_raw_salt):
    _df = mo.sql(
        f"""
        FROM forklift_raw_salt 
        SELECT month_number,"month",customer,SUM(total_price) AS total_salt_forklift_price,service

        WHERE report_type <> 'STO'
        GROUP BY ALL
        ORDER BY month_number
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
