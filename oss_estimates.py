import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")

with app.setup:
    import polars as pl
    import marimo as mo
    from datetime import date

    from data_source.sheet_ids import INVOICING,REPORT_STATUS
    from scan_google_sheet import scan_google_sheet
    from dataframe.stuffing import coa
    from dataframe.transport import transfer,shore_crane
    from dataframe.netlist import oss
    from dataframe.miscellaneous import cccs_stuffing



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
        WHERE status <> 'Send for Approval' AND number = 'CCCS OSS'
        """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Stuffing 🥙
    """)
    return


@app.cell(hide_code=True)
def _():
    stuffing_df = mo.sql(
        f"""
        FROM
            cccs_stuffing
        SELECT
            month(date) AS month_number,
            monthname(date) AS month_name,
            customer,
            CAST(SUM(total_tonnage) AS DECIMAL) AS total_tonnage,
            CAST(SUM(total_price) AS DECIMAL) AS total_price
        WHERE
            Invoiced = 'MAERSKLINE'
        GROUP BY ALL
        ORDER BY
            month_number
        """
    )
    return (stuffing_df,)


@app.cell(hide_code=True)
def _():
    shore_crane_df = mo.sql(
        f"""
        FROM shore_crane
            SELECT 
                month(date) AS month_number,
            monthname(date) AS month_name,
            REPLACE(REPLACE(customer,'S.A.',''),'S.A','') AS customer,
            CAST(SUM("hours") AS INTEGER) AS "total_hours",
            CAST(SUM(total_price) AS DECIMAL) AS total_price
    
        WHERE Invoiced_to = 'MAERSKLINE' AND operation_type = 'CCCS Container Stuffing'
        GROUP BY ALL
        ORDER BY month_number
        """
    )
    return (shore_crane_df,)


@app.cell
def _():
    electricity = mo.sql(
        f"""
        WITH monthly_bounds AS (
            SELECT
                month_start::DATE                                             AS month_start,
                (month_start + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS month_end
            FROM (
                SELECT unnest(generate_series(
                    '2026-01-01'::DATE,
                    '2026-12-01'::DATE,
                    INTERVAL '1 month'
                )) AS month_start
            )
        ),
        source AS (
            SELECT
                vessel_client,
                container_number,
                GREATEST(date_plugged, '2026-01-01'::DATE) AS effective_start,
                date_out,
                plugin_price,
                monitoring_price,
                electricity_unit_price
            FROM coa
            WHERE date_out IS NOT NULL
                AND operation_type = 'CCCS Stuffing - Basic OSS'
                AND YEAR(date_out) = 2026
        ),
        split AS (
            SELECT
                s.vessel_client,
                s.container_number,
                b.month_start                               AS invoice_month,
                GREATEST(s.effective_start, b.month_start)  AS period_start,
                LEAST(s.date_out, b.month_end)              AS period_end,
                s.plugin_price,
                s.monitoring_price,
                s.electricity_unit_price,
                -- Flag first and last month per container
                ROW_NUMBER() OVER (
                    PARTITION BY s.container_number ORDER BY b.month_start ASC
                ) AS rn_first,
                ROW_NUMBER() OVER (
                    PARTITION BY s.container_number ORDER BY b.month_start DESC
                ) AS rn_last
            FROM source s
            JOIN monthly_bounds b
                ON  s.effective_start <= b.month_end
                AND s.date_out        >= b.month_start
        ),final AS (
            SELECT
            vessel_client,
            container_number,
            invoice_month,
            period_start,
            period_end,
            (period_end - period_start)                                          AS days_on_plug,
            electricity_unit_price,
            -- Plugin only on first month
            CASE WHEN rn_first = 1 THEN plugin_price ELSE 0 END                  AS plugin_fee,
            -- Monitoring only on last month
            CASE WHEN rn_last  = 1 THEN monitoring_price ELSE 0 END              AS monitoring_fee,
            -- Electricity every month, prorated by days
            (period_end - period_start) * electricity_unit_price                 AS electricity_fee
        FROM split
        ORDER BY vessel_client, container_number, invoice_month
        ), add_total_price AS (
            FROM final
        SELECT *, (plugin_fee+monitoring_fee+electricity_fee) AS total_price

        )


        FROM add_total_price



        """,
        output=False
    )
    return (electricity,)


@app.cell
def _(electricity):
    haulage_df = mo.sql(
        f"""
        WITH
            last_record AS (
                SELECT
                    vessel_client,
                    TRIM(container_number) AS container_number ,
                    invoice_month,
                    period_end
                FROM
                    electricity
                QUALIFY
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            container_number
                        ORDER BY
                            period_end DESC
                    ) = 1
            ),
            transf AS (
                FROM
                    transfer
                SELECT
                    "date",
                    TRIM(container_number) AS container_number,
                    haulage_price
                WHERE
                    movement_type = 'Delivery'
                    AND remarks = 'MAERSKLINE'
                    AND status = 'Full'
            )

        SELECT
                MONTH(invoice_month) AS month_number,
            MONTHNAME(invoice_month) AS month_name,
            REPLACE(REPLACE(vessel_client,'S.A.',''),'S.A','') AS customer,
            COUNT(*) AS total_moves,
            CAST(SUM(haulage_price) AS DECIMAL) AS total_price
        FROM
            last_record l
            LEFT JOIN transf t ON l.container_number = t.container_number AND l.period_end = t."date"
        GROUP BY ALL
        ORDER BY month_number
        """
    )
    return (haulage_df,)


@app.cell(hide_code=True)
def _(electricity):
    electricity_df = mo.sql(
        f"""
        FROM
            electricity
        SELECT
            MONTH(invoice_month) AS month_number,
            MONTHNAME(invoice_month) AS month_name,
            REPLACE(REPLACE(vessel_client,'S.A.',''),'S.A','') AS customer,
            SUM(days_on_plug) AS total_days,
            CAST(SUM(total_price) AS DECIMAL) AS total_price
        GROUP BY ALL
        ORDER BY
            month_number
        """
    )
    return (electricity_df,)


@app.cell(hide_code=True)
def _(electricity_df, haulage_df, shore_crane_df, stuffing_df):
    _df = mo.sql(
        f"""
        WITH
            stuff AS (
                FROM
                    stuffing_df
            ),
            shore AS (
                FROM
                    shore_crane_df
            ),
            elec AS (
                FROM
                    electricity_df
            ),
            haul AS (
                FROM
                    haulage_df
            ),
            final AS (
                SELECT
                    s.* EXCLUDE (s.total_price),
                    COALESCE(x.total_hours, 0) AS total_hours,
                    COALESCE(e.total_days, 0) AS total_days,
                    COALESCE(h.total_moves, 0) AS total_moves,
                    s.total_price AS stuffing_price,
                    COALESCE(x.total_price, 0) AS shore_crane_price,
                    COALESCE(e.total_price, 0) AS electricity_price,
                    COALESCE(h.total_price, 0) AS haulage_price
                FROM
                    stuff s
                    LEFT JOIN shore x ON x.month_number = s.month_number
                    AND x.customer = s.customer
                    LEFT JOIN elec e ON s.month_number = e.month_number
                    AND e.customer = s.customer
                    LEFT JOIN haul h ON s.month_number = h.month_number
                    AND h.customer = s.customer
            )
        FROM
            final
        SELECT
            *,
            (
                stuffing_price + shore_crane_price + electricity_price + haulage_price
            ) AS total_price
        ORDER BY month_number,customer
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
