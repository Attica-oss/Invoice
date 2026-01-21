import marimo

__generated_with = "0.19.4"
app = marimo.App(width="medium")

with app.setup:
    import polars as pl
    import marimo as mo
    from data_source.make_dataset import load_excel
    from data_source.excel_file_path import ExcelFiles


@app.cell
def _():
    additional_stevedores_raw = load_excel(ExcelFiles.ADDITIONAL_OVERTIME)
    return (additional_stevedores_raw,)


@app.cell
def _(additional_stevedores_raw):
    _df = mo.sql(
        f"""
        SELECT
            Day_1 AS day_name,
            Date_1 AS service_date,
            UPPER(Vessel_1) AS vessel,
            ROUND(Tonnage, 3) AS tonnage,
            CAST("End Time" AS time) AS end_time,
            Hours_1 AS hours_worked,
            "Num of Stevedores" AS num_of_stevedores,
            Price_1 AS price,
            "Additional Stevedores ($)" AS additional_stevedores,
            "Remarks" AS remarks
        FROM
            additional_stevedores_raw
        WHERE day_name IS NOT NULL AND service_date BETWEEN '2025-10-01' AND '2025-12-31'
        ORDER BY date
        """
    )
    return


@app.cell
def _():
    extra_men_check = load_excel(ExcelFiles.EXTRA_MEN)
    return (extra_men_check,)


@app.cell
def _(extra_men_check):
    _df = mo.sql(
        f"""
        SELECT * FROM extra_men_check WHERE "Check" IS FALSE AND MONTH(Date) = 12
        """
    )
    return


@app.cell
def _():
    path,sheet = r"C:\Users\gmounac\Desktop\INPESCA.xlsx","Sheet1"
    return path, sheet


@app.cell
def _(path, sheet):
    inpesca = pl.read_excel(path,sheet_name=sheet)
    return (inpesca,)


@app.cell
def _(inpesca):
    cccs_report = mo.sql(
        f"""
        SELECT
            DATE as service_date,
            UPPER("SHIPOWNER/OPERATOR") AS vessel,
            "OPERATION TYPE" AS operation_type,
            CAST("TOTAL TONNAGE" AS FLOAT8) AS tonnage,
            COALESCE("Tipping tonnage  (Static Loader)",0) AS static_loader,
            "Tipping tonnage (Shore Crane)" AS shore_crane,
            COALESCE("IPHS OVERTIME TONNAGE IN/OUT",0) AS overtime_tonnage,
            "IPHS INVOICE" AS invoice_amount,
            "__UNNAMED__34" AS comments
        FROM
            inpesca
        WHERE service_date IS NOT NULL
        """
    )
    return (cccs_report,)


@app.cell
def _():
    from dataframe.miscellaneous import cccs_stuffing
    return (cccs_stuffing,)


@app.cell
def _(cccs_report, cccs_stuffing):
    compared = mo.sql(
        f"""
        WITH
            iphs_stuffing AS (
                SELECT
                    Day,
                    date,
                    Service,
                    SUM(total_tonnage) AS total_tonnage,
                    SUM(overtime_tonnage) AS overtime_tonnage,
                FROM
                    cccs_stuffing
                WHERE
                    date BETWEEN '2025-12-01' AND '2025-12-31'
                    AND customer = 'SAPMER'
                GROUP BY
                    Day,
                    date,
                    Service
            ),
            cccs_data AS (
                SELECT
                    *
                FROM
                    cccs_report
                WHERE
                    operation_type LIKE '%container%'
            ),
            iphs_data AS (
                SELECT
                    *,
                    CASE
                        WHEN Day IN ('Sun', 'PH')
                        AND Service LIKE '%Static%' THEN (
                            (total_tonnage - overtime_tonnage) * (3 + 3.5 ) * 1.5
                        ) + (overtime_tonnage * (3+ 3.5) * 2)
                        WHEN Day NOT IN ('Sun', 'PH')
                        AND Service LIKE '%Static%' THEN (
                            (total_tonnage - overtime_tonnage) * (3+ 3.5 ) * 1.0
                        ) + (overtime_tonnage * (3 + 3.5) * 1.5)
                        WHEN Day IN ('Sun', 'PH')
                        AND Service LIKE '%Shore%' THEN (
                            (total_tonnage - overtime_tonnage) * ( 1.5+ 3.5) * 1.5
                        ) + (overtime_tonnage * (1.5+ 3.5) * 2)
                        WHEN Day NOT IN ('Sun', 'PH')
                        AND Service LIKE '%Shore%' THEN (
                            (total_tonnage - overtime_tonnage) * (1.5+ 3.5) * 1.0
                        ) + (overtime_tonnage * ( 1.5+ 3.5) * 1.5)
                        ELSE 0
                    END AS price
                FROM
                    iphs_stuffing
                ORDER BY
                    date
            )

        SELECT * FROM iphs_data
        -- SELECT
        --     c.service_date AS cccs_date,
        --     c.vessel AS vessel,
        --     c.tonnage,
        --     c.static_loader,
        --     c.shore_crane,
        --     c.overtime_tonnage,
        --     ROUND(c.invoice_amount, 3) AS invoice_amount,
        --     i.date AS iphs_date,
        --     i.Service AS service,
        --     i.total_tonnage,
        --     i.overtime_tonnage AS iphs_ot_tonnage,
        --     ROUND(i.price, 3) AS total_amount
        -- FROM
        --     cccs_data c
        --     FULL JOIN iphs_data i ON i.date = c.service_date
        """
    )
    return


@app.cell
def _():
    641.979 - 567.98
    return


@app.cell
def _():
    _df = mo.sql(
        f"""

        """
    )
    return


@app.cell
def _():
    from dataframe.miscellaneous import misc
    return (misc,)


@app.cell
def _(cccs_report, misc):
    xxx = mo.sql(
        f"""
        WITH cccs AS (SELECT service_date,vessel,tonnage,overtime_tonnage, invoice_amount,comments FROM cccs_report WHERE operation_type NOT LIKE '%container%'), iphs AS (SELECT
            date,
            vessel,
            total_tonnage,
            overtime_tonnage,
            ((total_tonnage - overtime_tonnage) * 3.5) + (overtime_tonnage * 3.5 *1.5) AS price
        FROM
            misc
        WHERE
            date BETWEEN '2025-12-01' AND '2025-12-31'
            AND customer LIKE '%SAPMER%'
            AND operation_type NOT LIKE '%Container%')

        SELECT * FROM iphs
        """
    )
    return (xxx,)


@app.cell
def _(xxx):
    _df = mo.sql(
        f"""
        SELECT SUM(price) FROM xxx WHERE date BETWEEN '2025-12-06' AND '2025-12-11'
        """
    )
    return


@app.cell
def _():
    from utils.google_sheet import GoogleSheetsLoader
    return (GoogleSheetsLoader,)


@app.cell
def _(GoogleSheetsLoader):
    gs = GoogleSheetsLoader()

    return (gs,)


@app.cell
def _(gs):
    by_c_driver = gs.load_sheet(config_name="By_Catch_Transfer_Driver").data
    return (by_c_driver,)


@app.cell
def _(by_c_driver):
    _df = mo.sql(
        f"""
        SELECT * FROM by_c_driver WHERE date > '2025-10-31' AND is_in_record <> 'OK'
        """
    )
    return


@app.cell
def _():
    mo.md(r"""
    14/11 - Has a record and even overtime.
    18/11 - No Ocean Basket record for Playa de Azkorri
    22/11 - Has a record.
    06/12 - Has a record.
    18/12 - Has a record.
    22/12 - Has a record
    """)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
