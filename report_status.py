import marimo

__generated_with = "0.20.1"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    from data_source.excel_file_path import ExcelFiles
    from data_source.make_dataset import load_excel,load_gsheet_data
    from data_source.sheet_ids import INVOICE_STATUS,INVOICING


@app.cell
def _():
    # Berth Dues - 2026

    berth_df = load_excel(file_path=ExcelFiles.BERTH_DUES_2026)
    return (berth_df,)


@app.cell(hide_code=True)
def _(berth_df):
    _df = mo.sql(
        f"""
        FROM berth_df
        SELECT "VESSEL NAME" AS vessel,"DATE IN" AS date_in,CAST("TIME IN" AS TIME) AS time_in,"DATE OUT" AS date_out,CAST("TIME OUT" AS TIME) AS time_out,"INVOICE VALUE" AS invoice_value,"VESSEL TYPE" AS vessel_type
        """
    )
    return


@app.cell
def _():
    df_test = load_gsheet_data(INVOICING,INVOICE_STATUS)
    return (df_test,)


@app.cell(hide_code=True)
def _(df_test):
    _df = mo.sql(
        f"""
        FROM df_test
        WHERE month LIKE '%February%' AND YEAR(starting_date) = 2026
        """
    )
    return


@app.cell
def _():
    from dataframe.transport import transfer
    from dataframe.stuffing import coa

    return coa, transfer


@app.cell(hide_code=True)
def _(transfer):
    _df = mo.sql(
        f"""
        FROM transfer WHERE line = 'CMA CGM' AND (remarks = 'CMA CGM' AND status = 'Full')
        """
    )
    return


@app.cell(hide_code=True)
def _(coa):
    cma_electricity = mo.sql(
        f"""
        FROM
            coa
        SELECT
            vessel_client || ' // ' || customer AS customer,
            date_plugged + time_plugged AS date_plugged,
            container_number,
            set_point,
            date_out,
            days_on_plug,
            plugin_price,
            monitoring_price,
            electricity_unit_price,
            total_electricity,
            total,
            CASE WHEN location = 'LML' THEN ''
         ELSE 'Cross Stuffed' END AS remarks
        WHERE
            customer = 'CMA CGM'
        """
    )
    return (cma_electricity,)


@app.cell(hide_code=True)
def _(cma_electricity):
    _df = mo.sql(
        f"""
        WITH agg AS (
          SELECT
            set_point,
            SUM(days_on_plug)       AS days_on_plug,
            SUM(plugin_price)       AS plugin_price,
            SUM(monitoring_price)   AS monitoring_price,
            SUM(total_electricity)  AS total_electricity
          FROM cma_electricity
          GROUP BY set_point
        ),
        tot AS (
          SELECT
            SUM(plugin_price)      AS plugin_total,
            SUM(monitoring_price)  AS monitoring_total,
            SUM(total_electricity) AS electricity_total
          FROM agg
        )
        SELECT
          service,
          no,
          price
        FROM (
          -- PLUGIN row
          SELECT
            'PLUGIN' AS service,
            CAST(SUM(days_on_plug) AS VARCHAR) || ' plugins' AS no,
            SUM(plugin_price) AS price,
            1 AS sort_key
          FROM agg

          UNION ALL

          -- MONITORING row
          SELECT
            'MONITORING' AS service,
            CAST(SUM(days_on_plug) AS VARCHAR) || ' monitorings' AS no,
            SUM(monitoring_price) AS price,
            2 AS sort_key
          FROM agg

          UNION ALL

          -- ELECTRICITY rows (one per set_point)
          SELECT
            'ELECTRICITY ' || CAST(set_point AS VARCHAR) || '°' AS service,
            CAST(days_on_plug AS VARCHAR) || ' days' AS no,
            total_electricity AS price,
            10 + ROW_NUMBER() OVER (ORDER BY set_point) AS sort_key
          FROM agg

          UNION ALL

          -- Total row
          SELECT
            'Total' AS service,
            '' AS no,
            (plugin_total + monitoring_total + electricity_total) AS price,
            999 AS sort_key
          FROM tot
        )
        ORDER BY sort_key;
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
