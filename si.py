import marimo

__generated_with = "0.19.2"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells

    import marimo as mo
    import polars as pl
    from utils.google_sheet import GoogleSheetsLoader


@app.cell
def _():
    gs = GoogleSheetsLoader()
    return (gs,)


@app.cell
def _(gs):
    report = gs.load_sheet(config_name="validation").data
    return (report,)


@app.cell
def _(report):
    _df = mo.sql(
        f"""
        SELECT
            report_type,
            sub_type,
            report_name,
            customer,
            starting_date,
            ending_date,
            invoice_total,
            remarks
        FROM
            report
        WHERE
            (
                report_type = 'si'
                OR (
                    report_type = 'oss'
                    AND sub_type = 'CCCS OSS'
                )
                OR report_type = 'monthly'
            ) AND sub_type NOT NULL
            AND month LIKE '%December%'
        """
    )
    return


@app.cell
def _():
    from dataframe.transport import forklift
    from dataframe.transport import transfer
    return (transfer,)


@app.cell
def _():
    from dataframe.stuffing import coa
    return (coa,)


@app.cell
def _(coa, transfer):
    ops = mo.sql(
        f"""
        WITH
            stuffing AS (
                SELECT
                    -- *,
                    date_plugged,
                    container_number,
            plugged_status,
                    CASE
                        WHEN (
                            date_out > '2025-12-31'
                            OR date_out IS NULL
                        ) THEN '2025-12-31'
                        ELSE date_out
                    END AS end_date,
                    CASE
                        WHEN plugged_status = 'Partial' THEN DATEDIFF('days', date_plugged, end_date)
                        ELSE DATEDIFF('days', date_plugged, end_date) + 1
                    END AS days,
                    plugin_price,
                    CASE
                        WHEN (
                            date_out > '2025-12-31'
                            OR date_out IS NULL
                        ) THEN 0
                        ELSE monitoring_price
                    END AS monitoring,
                    (days * electricity_unit_price) AS electricity_price,
                    (monitoring + plugin_price + electricity_price) AS total
                FROM
                    coa
                WHERE
                    vessel_client = 'OCEAN BASKET'
                    AND (customer <> 'MAERSKLINE')
                    AND (date_plugged < '2025-12-31')
                    AND (
                        date_out IS NULL
                        OR date_out > '2025-12-01'
                    )
            ),
            trans AS (
                SELECT
                    *
                FROM
                    transfer
                WHERE
                    movement_type = 'Delivery'
                    AND Status = 'Full'
            )

        SELECT * FROM stuffing s LEFT JOIN trans t ON s.end_date = t.date AND s.container_number = t.container_number
        """
    )
    return (ops,)


@app.cell
def _(ops):
    _df = mo.sql(
        f"""
        SELECT SUM(total),SUM(haulage_price) FROM ops
        """
    )
    return


@app.cell
def _():
    (5.518 * 11) + 135
    return


@app.cell
def _(cross_stuffing):
    _df = mo.sql(
        f"""
        SELECT * FROM cross_stuffing WHERE date BETWEEN '2025-12-01' AND '2025-12-31'
        """
    )
    return


@app.cell
def _():
    1_021.46225 + 2_260.1585 + 	2_685 + 270
    return


@app.cell
def _():
    from dataframe.miscellaneous import by_catch
    return (by_catch,)


@app.cell
def _(by_catch):
    _df = mo.sql(
        f"""
        SELECT SUM(total_price) FROM by_catch WHERE date BETWEEN '2025-12-01' AND '2025-12-31' AND customer = 'OCEAN BASKET'
        """
    )
    return


@app.cell
def _():
    from dataframe.bin_dispatch import full_scows
    return (full_scows,)


@app.cell
def _(full_scows):
    _df = mo.sql(
        f"""
        SELECT
            SUM(tonnage) * 3.5
        FROM
            full_scows
        WHERE
            date BETWEEN '2025-12-01' AND '2025-12-31'
        """
    )
    return


@app.cell
def _():
    7302 + 4_778.157999999999 + 4_778.157999999999
    return


@app.cell
def _():
    text=mo.md(r"""
    Good morning.<br>
    I hope you will get the help you clearly need.<br>
    We may share a surname, but make no mistake—**we are no longer family**.<br>
    "Loose lips sink ships." Remember that.<br>
    This is the last time you'll hear from me. I don't forgive, and I don't forget either.<br>
    Get yourself sorted out—**Adieu**.<br>
    """)
    return (text,)


@app.cell
def _(text):
    mo.hstack([mo.image(src="/home/attica/Downloads/Untitled.jpeg",height=500),text],gap=1,justify='center')
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
