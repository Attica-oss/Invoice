import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells
    import marimo as mo
    import polars as pl
    from utils.google_sheet import GoogleSheetsLoader


@app.cell
def _():
    from dataframe.netlist import netList
    from dataframe.stuffing import coa
    from dataframe.transport import forklift,transfer,shore_crane
    from dataframe.shore_handling import salt
    from dataframe.miscellaneous import cross_stuffing
    from dataframe.netlist import iot_cargo
    from dataframe.emr import shifting
    return (
        coa,
        cross_stuffing,
        forklift,
        iot_cargo,
        salt,
        shifting,
        shore_crane,
        transfer,
    )


@app.cell
def _(shore_crane):
    _df = mo.sql(
        f"""
        SELECT * FROM shore_crane WHERE MONTH(date) = 11
        """
    )
    return


@app.cell
def _(shifting):
    _df = mo.sql(
        f"""
        SELECT * FROM shifting WHERE date BETWEEN '2025-10-01' AND '2025-11-30'
        """
    )
    return


@app.cell
def _(iot_cargo):
    iot_cargo_df = mo.sql(
        f"""
        SELECT * FROM iot_cargo WHERE vessel = 'DOLOMIEU'
        """
    )
    return (iot_cargo_df,)


@app.cell
def _(iot_cargo_df):
    iot_cargo_df.select(pl.col("total_price").sum().round())
    return


@app.cell
def _(cross_stuffing):
    _df = mo.sql(
        f"""
        SELECT * FROM cross_stuffing WHERE date between '2025-10-01' AND '2025-11-30'
        """
    )
    return


@app.cell
def _():
    1_337.22 + 1_672.8
    return


@app.cell
def _(cross_stuffing):
    _df = mo.sql(
        f"""
        WITH cross_stuff AS (SELECT
            *
        FROM
            cross_stuffing
        WHERE
            date BETWEEN '2025-11-01' AND '2025-11-30')

        SELECT Service,invoiced, SUM(total_price) AS total_price FROM cross_stuff GROUP BY Service,invoiced
        """
    )
    return


@app.cell
def _(salt):
    _df = mo.sql(
        f"""
        SELECT * FROM salt  WHERE vessel = 'ADAMAS' AND date BETWEEN  '2025-10-29' AND '2025-11-10'
        """
    )
    return


@app.cell
def _(forklift):
    _df = mo.sql(
        f"""
        SELECT * FROM forklift WHERE customer = 'ADAMAS' AND date BETWEEN '2025-10-29' AND '2025-11-10'
        """
    )
    return


@app.cell
def _():
    search_container = mo.ui.text(max_length=11)
    search_container
    return (search_container,)


@app.cell
def _(coa, search_container):
    _df = mo.sql(
        f"""
        SELECT * FROM coa WHERE container_number = '{search_container.value}'
        """
    )
    return


@app.cell
def _(coa):
    _df = mo.sql(
        f"""
        SELECT
            customer,
            MAKE_TIMESTAMP(
                YEAR(date_plugged),
                MONTH(date_plugged),
                DAY(date_plugged),
                HOUR(time_plugged),
                MINUTE(time_plugged),
                SECOND(time_plugged)
            ) AS datetime_plugging,
            container_number,
            operation_type,
            days_on_plug,
            date_out,
            total
        FROM
            coa
        WHERE
            customer = 'IOT IMPORT' AND MONTH(datetime_plugging) = 11
        """
    )
    return


@app.cell
def _(netlist, search_container):
    _df = mo.sql(
        f"""
        SELECT * FROM netList WHERE destination = '{search_container.value}'
        """
    )
    return


@app.cell
def _(search_container, transfer):
    _df = mo.sql(
        f"""
        SELECT * FROM transfer WHERE container_number = '{search_container.value}'
        """
    )
    return


@app.cell
def _(transfer):
    _df = mo.sql(
        f"""
        SELECT * FROM transfer WHERE remarks = 'IOT IMPORT' AND MONTH(date) = 11
        """
    )
    return


@app.cell
def _():
    gs = GoogleSheetsLoader()
    return (gs,)


@app.cell
def _(gs):
    pti_log_dataf = gs.load_sheet(config_name="container_pti_log").data

    gate_in_dataf = gs.load_sheet(config_name="container_gate_in").data

    washing_log_dataf = gs.load_sheet(config_name="container_cleaning_log").data

    cross_stuffing_dataf = gs.load_sheet(config_name="cross_stuffing").data
    return (
        cross_stuffing_dataf,
        gate_in_dataf,
        pti_log_dataf,
        washing_log_dataf,
    )


@app.cell
def _(pti_log_dataf):
    pti_log_dataf
    return


@app.cell
def _(pti_log_dataf):
    pti_log_df = mo.sql(
        f"""
        SELECT
            "Date Plugin" AS date_plugin,
            "Time Plugin" AS time_plugin,
            "Container Number" AS container_number,
            "Set Point" AS set_point,
            "Unit Manufacturer" AS unit_manufacturer,
            "Shipping Line" AS shipping_line,
            "Unplugged Date" AS unplugged_date,
            Sticker,
            Status
        FROM
            pti_log_dataf
        """
    )
    return (pti_log_df,)


@app.cell
def _(gate_in_dataf):
    gate_in_df = mo.sql(
        f"""
        SELECT
            Date,
            Time,
            "Container Number" AS container_number,
            "Shipping Line" AS shipping_line,
            Type,
            "Unit Manufacturer" AS unit_manufacturer,
            Status,
            "PTI Status" AS pti_status
        FROM
            gate_in_dataf
        WHERE container_number = 'TLLU1034465'
        """
    )
    return (gate_in_df,)


@app.cell
def _(washing_log_dataf):
    washing_df = mo.sql(
        f"""
        SELECT
            timestamp,
            date,
            container_number,
            shipping_line,
            cleaning_remarks,
            "Invoice To"
        FROM
            washing_log_dataf
        WHERE
            date BETWEEN '2025-10-01' AND '2025-12-30' AND "Invoice To" LIKE '%SAPMER%'
        """
    )
    return (washing_df,)


@app.cell
def _(cross_stuffing_dataf):
    cross_stuffing_df = mo.sql(
        f"""
        SELECT
            date,
            origin,
            end_time,
            invoiced 
        FROM
            cross_stuffing_dataf
        WHERE
            is_origin_empty
            AND date BETWEEN '2025-10-01' AND '2025-11-30'
        """
    )
    return (cross_stuffing_df,)


@app.cell
def _():
    shipping_line_selector = mo.ui.dropdown(options=['MAERSK','IOT','CMA CGM'],value='IOT')
    shipping_line_selector
    return (shipping_line_selector,)


@app.cell
def _(cross_stuffing_df, washing_df):
    _df = mo.sql(
        f"""
        WITH
            x_stuff AS (
                SELECT
                    *
                FROM
                    cross_stuffing_df
            ),
            wash AS (
                SELECT
                    *
                FROM
                    washing_df
            )

        SELECT * FROM x_stuff x FULL OUTER JOIN wash w ON x.origin = w.container_number AND x.date <= w.date
        """
    )
    return


@app.cell
def _(gate_in_dataf, washing_df):
    wash_view = mo.sql(
        f"""
        WITH
            gate AS (
                SELECT
                    Date,
                    Time,
                    "Container Number" AS container_number,
                    "Shipping Line" AS shipping_line,
                    -- Type,
                    -- "Unit Manufacturer" AS unit_manufacturer,
                    Status,
                    "PTI Status" AS pti_status
                FROM
                    gate_in_dataf
            ),
            wash AS (
                SELECT
                    *
                FROM
                    washing_df
            )
        SELECT
            *,
            (g.shipping_line = w.shipping_line) AS line_check
        FROM
            wash w
            JOIN gate g ON g.Date <= w.date
            AND g.container_number = w.container_number
        """
    )
    return (wash_view,)


@app.cell
def _(wash_view):
    wash_view.filter(pl.col("container_number").is_duplicated()).sort(['container_number','date'])
    return


@app.cell
def _(gate_in_df, pti_log_df, shipping_line_selector):
    view = mo.sql(
        f"""
        WITH
            pti AS (
                SELECT
                    *
                FROM
                    pti_log_df
                WHERE
                    date_plugin BETWEEN '2025-11-01' AND '2025-11-30'
                    AND shipping_line = '{shipping_line_selector.value}'
            ),
            gate AS (
                SELECT
                    * FROM gate_in_df
                WHERE
                    date <= '2025-11-30'
            )
        SELECT
            *,
            p.unit_manufacturer = g.unit_manufacturer AS unit_check
        FROM
            pti p
            LEFT JOIN gate g ON p.date_plugin >= g.date
            AND p.container_number = g.container_number
        """
    )
    return (view,)


@app.cell
def _(view):
    _df = mo.sql(
        f"""
        SELECT
            date_plugin,
            time_plugin,
            container_number,
            set_point,
            CASE WHEN 
            	set_point = -25 THEN 'Standard'
            	WHEN set_point = -35 THEN 'Magnum'
            	WHEN set_point = -60 THEN 'S Freezer'
            ELSE NULL END AS type_on_pti,
            -- unit_manufacturer,
            Date AS gate_in_date,
            Time AS gate_in_time,
            Type,
           -- unit_manufacturer_1 as gate_in_manufacturer
        FROM
            view
        WHERE
            type_on_pti <> Type
        """
    )
    return


@app.cell
def _(pti_log_dataf, shipping_line_selector):
    _df = mo.sql(
        f"""
        SELECT
            -- "Date Plugin"::TIMESTAMP + "Time Plugin"::TIMESTAMP  
            MAKE_TIMESTAMP(YEAR("Date Plugin"),month("Date Plugin"),
                day("Date Plugin"),
                hour("Time Plugin"),
                minute("Time Plugin"),
                second("Time Plugin"))AS date_plugin,
            "Container Number" AS container_number,
            "Set Point" AS set_point,
            "Unit Manufacturer" AS unit_manufacturer,
            "Shipping Line" AS shipping_line,
            "Unplugged Date" AS unplugged_date,
            Sticker,
            Status,
           (EXTRACT(EPOCH FROM (unplugged_date - date_plugin)) / 3600 ) AS hours_diff 
        FROM
            pti_log_dataf
        WHERE
            "Date Plugin" BETWEEN '2025-10-01' AND '2025-11-30'
            AND "Shipping Line" = '{shipping_line_selector.value}'
        	AND hours_diff > 20
        """
    )
    return


@app.cell
def _():
    return


@app.cell
def _(pti_log_dataf, shipping_line_selector):
    test_duplicate = mo.sql(
        f"""
        SELECT
            -- "Date Plugin"::TIMESTAMP + "Time Plugin"::TIMESTAMP  
            MAKE_TIMESTAMP(YEAR("Date Plugin"),month("Date Plugin"),
                day("Date Plugin"),
                hour("Time Plugin"),
                minute("Time Plugin"),
                second("Time Plugin"))AS date_plugin,
            "Container Number" AS container_number,
            "Set Point" AS set_point,
            "Unit Manufacturer" AS unit_manufacturer,
            "Shipping Line" AS shipping_line,
            "Unplugged Date" AS unplugged_date,
            Sticker,
        FROM
            pti_log_dataf
        WHERE
            "Date Plugin" BETWEEN '2025-10-01' AND '2025-11-30'
            AND "Shipping Line" = '{shipping_line_selector.value}' ---AND container_number = 'SUDU6038825'
        """
    )
    return (test_duplicate,)


@app.cell
def _(test_duplicate):
    test_duplicate.filter(pl.col("container_number").is_duplicated()).sort(["container_number",'date_plugin'])

    #.and_(pl.col("date_plugin").dt.month().eq(11))
    return


@app.cell
def _(pti_log_dataf, shipping_line_selector):
    _df = mo.sql(
        f"""
        SELECT
            -- "Date Plugin"::TIMESTAMP + "Time Plugin"::TIMESTAMP  
            MAKE_TIMESTAMP(YEAR("Date Plugin"),month("Date Plugin"),
                day("Date Plugin"),
                hour("Time Plugin"),
                minute("Time Plugin"),
                second("Time Plugin"))AS date_plugin,
            "Container Number" AS container_number,
            Size,
            "Set Point" AS set_point,
            "Unit Manufacturer" AS unit_manufacturer,
               "Unplugged Date" AS unplugged_date,
                Sticker,
            Status,
            "Shipping Line" AS shipping_line,
         Generator

        FROM
            pti_log_dataf
        WHERE
            "Date Plugin" BETWEEN '2025-11-11' AND '2025-12-30'
            AND "Shipping Line" = '{shipping_line_selector.value}'
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
