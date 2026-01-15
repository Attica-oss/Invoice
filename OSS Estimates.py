import marimo

__generated_with = "0.19.2"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells
    import polars as pl
    from utils.google_sheet import GoogleSheetsLoader
    import marimo as mo

    from data_source.make_dataset import ExcelFiles,load_excel


@app.cell
def _():
    gs = GoogleSheetsLoader()
    return (gs,)


@app.cell
def _(gs):
    report = gs.load_sheet(config_name="validation", parse_dates=True).data
    return (report,)


@app.cell
def _(report):
    full_oss_dataf = mo.sql(
        f"""
        SELECT
            ROW_NUMBER() OVER (ORDER BY starting_date) AS id,
            REPLACE(report_name,' - OSS','') AS vessel,
            -- sub_type AS id,
            customer,
            starting_date,
            ending_date
            -- STRPTIME(ending_date, '%d/%m/%Y')::DATE AS ending_date
        FROM
            report
        WHERE
            report_type = 'oss'
            AND sub_type = 'FULL OSS'
            AND month LIKE '%December%'
        """
    )
    return (full_oss_dataf,)


@app.cell
def _():
    from dataframe.stuffing import coa
    return (coa,)


@app.cell
def _(coa, full_oss_dataf):
    stuffing_dataf = mo.sql(
        f"""
        WITH
            oss AS (
                SELECT
                    *
                FROM
                    full_oss_dataf
            ),
            stuffing AS (
                SELECT
                    -- *,
            vessel_client AS vessel,
            date_plugged AS start_date,
            container_number,
            set_point,
            CASE WHEN YEAR(date_out) = 2026 OR date_out IS NULL THEN '2025-12-31'  ELSE date_out
             END AS end_date,
            plugged_status AS status,
            CASE WHEN status = 'Partial' THEN DATEDIFF('days',start_date,end_date) ELSE DATEDIFF('days',start_date,end_date) +1
            END AS days_on_plug,
            plugin_price,
            CASE WHEN YEAR(date_out) = 2026 OR date_out IS NULL THEN 0 ELSE monitoring_price
            END AS monitoring_price,
            (days_on_plug * electricity_unit_price) AS electricity_price
                FROM
                    coa
            WHERE customer = 'MAERSKLINE' AND YEAR(start_date) = 2025
            ),joined AS (

            SELECT
            o.id,
            o.vessel,
            s.container_number,
            s.plugin_price,
            s.monitoring_price,
            s.electricity_price,
            ( s.plugin_price + s.monitoring_price + s.electricity_price) AS total
        FROM
            oss o
            LEFT JOIN stuffing s ON s.start_date BETWEEN o.starting_date AND o.ending_date
            AND s.vessel = o.vessel
        WHERE s.days_on_plug IS NOT NULL
            )

        SELECT id,vessel, SUM(total) AS total FROM joined GROUP BY id,vessel

        """
    )
    return (stuffing_dataf,)


@app.cell
def _():
    from dataframe.transport import shore_crane
    return (shore_crane,)


@app.cell
def _(full_oss_dataf, shore_crane):
    crane_dataf = mo.sql(
        f"""
        WITH oss AS (SELECT * FROM full_oss_dataf),crane AS (SELECT date,customer AS vessel,invoiced_to,total_price FROM shore_crane)


        SELECT * FROM oss o LEFT JOIN crane c ON c.date BETWEEN o.starting_date AND o.ending_date AND o.vessel = c.vessel
        """
    )
    return


@app.cell
def _():
    from dataframe.netlist import oss
    return (oss,)


@app.cell
def _(full_oss_dataf, oss):
    net_dataf = mo.sql(
        f"""
        WITH
            oss_ AS (
                SELECT
                    *
                FROM
                    full_oss_dataf
            ),
            net AS (
                SELECT
                    date,
                    vessel,
                    service,
                    invoice_value AS total
                FROM
                    oss
            WHERE YEAR(date) = 2025
            ), joined AS (

            SELECT
            *
        FROM
            oss_ o
            LEFT JOIN net n ON n.date BETWEEN o.starting_date AND o.ending_date
            AND o.vessel = n.vessel
        WHERE service IS NOT NULL
            )


    
        SELECT id,vessel,ROUND(SUM(total),3) AS total FROM joined GROUP BY id,vessel

        """
    )
    return (net_dataf,)


@app.cell
def _():
    movement_out = load_excel(ExcelFiles.MOVEMENT_OUT)
    return (movement_out,)


@app.cell
def _():
    from dataframe.transport import transfer
    return (transfer,)


@app.cell
def _(full_oss_dataf, movement_out, transfer):
    haulage_dataf = mo.sql(
        f"""
        WITH
            oss AS (
                SELECT
                    *
                FROM
                    full_oss_dataf
            ),
            move_out AS (
                SELECT
                    Date AS date,
                    "Container Ref. No." AS container_number,
                    UPPER("F/Vessel") AS vessel
                FROM
                    movement_out
                WHERE
                    Status = 'F'
            ),
            trans AS (
                SELECT
                    date,
                    container_number,
                    haulage_price
                FROM
                    transfer
                WHERE
                    remarks = 'MAERSKLINE'
                    AND status = 'Full'
                    AND movement_type = 'Delivery'
                    AND YEAR(date) = 2025
            ), mov_trans AS (
        SELECT
            *
        FROM
            move_out m
            JOIN trans t ON t.date = m.date
            AND t.container_number = m.container_number
    
            ),joined AS (

    
        SELECT * FROM oss o JOIN mov_trans mv ON mv.date > o.starting_date AND mv.vessel = o.vessel
            )


        SELECT id,vessel,SUM(haulage_price) AS total FROM joined GROUP BY id,vessel


        """
    )
    return (haulage_dataf,)


@app.cell
def _(haulage_dataf, net_dataf):
    net_dataf
    haulage_dataf

    return


@app.cell
def _(full_oss_dataf, haulage_dataf, net_dataf, stuffing_dataf):
    _df = mo.sql(
        f"""
        WITH
            oss AS (
                SELECT
                    *
                FROM
                    full_oss_dataf
            ),
            stuff AS (
                SELECT
                    *
                FROM
                    stuffing_dataf
            ),
            net AS (
                SELECT
                    *
                FROM
                    net_dataf
            ),
            trans AS (
                SELECT
                    *
                FROM
                    haulage_dataf
            )
        SELECT
            o.id,
            o.vessel,
            o.starting_date,
            o.ending_date,
            COALESCE(s.total, 0) AS electricity_price,
            COALESCE(n.total, 0) AS stuffing_price,
            COALESCE(t.total, 0) AS haulage_price
        FROM
            oss o
            LEFT JOIN stuff s ON s.id = o.id
            LEFT JOIN net n ON n.id = o.id
            LEFT JOIN trans t ON t.id = o.id
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
