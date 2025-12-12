import marimo

__generated_with = "0.18.3"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells
    import polars as pl
    import marimo as mo
    from dataframe.netlist import netList
    from dataframe.miscellaneous import dispatch_to_cargo
    from utils.google_sheet import GoogleSheetsLoader


@app.cell
def _():
    3*45
    return


@app.cell
def _():
    from dataframe.shore_handling import salt,forklift_salt
    return forklift_salt, salt


@app.cell
def _(forklift_salt):
    f_salt = forklift_salt()
    return (f_salt,)


@app.cell
def _(f_salt):
    _df = mo.sql(
        f"""
        SELECT * FROM f_salt WHERE date BETWEEN '2025-09-01' AND '2025-09-30' AND vessel = 'ALBACORA CUATRO'
        """
    )
    return


@app.cell
def _(salt):
    _df = mo.sql(
        f"""
        SELECT * FROM salt WHERE date BETWEEN '2025-09-01' AND '2025-09-30' AND vessel = 'ALBACORA CUATRO'
        """
    )
    return


@app.cell
def _():
    cold_store_dataf = GoogleSheetsLoader().load_sheet(config_name="CCCS_Activity").data

    transfer_dataf = GoogleSheetsLoader().load_sheet(config_name="Transfer").data
    return cold_store_dataf, transfer_dataf


@app.cell
def _():
    stdu_transfer = r"C:\Users\gmounac\Dropbox\Container and Transport\Transport Section\Container Movements\STDU Transfer.xlsx"

    transfer = r"C:\Users\gmounac\Dropbox\Container and Transport\Transport Section\Container Movements\Container Transfer.xlsx"

    shifting = r"C:\Users\gmounac\Dropbox\Container and Transport\Transport Section\Container Shifting Records\IPHS Container Shifting Record.xlsx"

    forklift_path: str = (
        r"C:\Users\gmounac\Dropbox\Container and Transport\Transport Section\Forklift Usage\Forklift Record.xlsx"
    )

    salt_path:str = (
        r"C:\Users\gmounac\Dropbox\Container and Transport\Salt Handling\IPHS Salt Operations.xlsx"
    )


    storage = r"P:\Verification & Invoicing\Validation Report\Validation Report - 2024.xlsx"
    return forklift_path, salt_path, shifting, stdu_transfer, storage, transfer


@app.cell
def _(storage):
    storage_dataf = pl.read_excel(
        storage, sheet_name="movement_in", schema_overrides={"Time in": pl.Time}
    )
    return (storage_dataf,)


@app.cell
def _(forklift_path: str, shifting, stdu_transfer, transfer):
    stdu_dataf = pl.read_excel(stdu_transfer,schema_overrides={"Time out":pl.Time,"Time in":pl.Time})

    transfer_log_dataf = pl.read_excel(transfer,sheet_name="Transfer",schema_overrides={"Time out":pl.Time,"Time in":pl.Time})

    shifting_log_dataf = pl.read_excel(shifting)

    forklift_log_dataf = pl.read_excel(forklift_path,sheet_name="Forklift_Operation",schema_overrides={"Time In":pl.Time,"Time Out":pl.Time,"Duration":pl.Time})
    return (
        forklift_log_dataf,
        shifting_log_dataf,
        stdu_dataf,
        transfer_log_dataf,
    )


@app.cell
def _(salt_path: str):
    salt_dataf = pl.read_excel(salt_path,sheet_name="Salt Operations")
    return (salt_dataf,)


@app.cell
def _(salt_dataf):
    _df = mo.sql(
        f"""
        SELECT * FROM salt_dataf WHERE Vessel = 'Izurdia' AND YEAR("Tue 17/01") = 2025 AND MONTH("Tue 17/01") = 9
        """
    )
    return


@app.cell
def _():
    pdf_browser = mo.ui.file_browser(initial_path=r"P:\Logistics\DOCUMENTATION - Logistics and Transport\Filling Sheets",multiple=False, )
    pdf_browser
    return (pdf_browser,)


@app.cell
def _(pdf_browser):
    # Get path of first selected file
    path = pdf_browser.path(0)  # this returns a Path or None

    if path is None:
        mo.md("No file selected.")
    else:
        # with open(path, "rb") as f:
        #     pdf_bytes = f.read()

        file = mo.pdf(src=path)
    return (file,)


@app.cell
def _(file):
    file
    return


@app.cell
def _():
    from dataframe.emr import washing
    return (washing,)


@app.cell
def _(washing):
    washing.collect()
    return


@app.cell
def _():
    mo.md(r"""
    ### Forklift
    """)
    return


@app.cell
def _(forklift_log_dataf):
    _df = mo.sql(
        f"""
        ---('ALAKRANA','ALAKRANTXU','ATERPE ALAI','ECHEBASTAR','ELAI ALAI','TXORI BAT')
        ---('EUSKADI ALAI','HARTSWATER','IZARO','JAI ALAI')
        SELECT
            *
        FROM
            forklift_log_dataf
        WHERE
            "Date of Service" BETWEEN '2025-09-01' AND '2025-09-30'
           -- AND UPPER("Vessel/Client") IN ('EUSKADI ALAI','HARTSWATER','IZARO','JAI ALAI')
            AND Purpose NOT LIKE '%Salt%' 
        ORDER BY "Date of Service","Time Out"
        """
    )
    return


@app.cell
def _(storage_dataf):
    _df = mo.sql(
        f"""
        WITH
            month_storage AS (
                SELECT
                    *
                FROM
                    storage_dataf
                WHERE
                    (
                        "Date Out" BETWEEN '2025-09-01' AND '2025-09-30'
                        OR "Date Out" IS NULL
                        OR "Date Out" > '2025-09-30'
                    )
                    AND "Date Out" < '2025-09-30'
                    AND "Line/Client" = 'IOT'
                    AND Status = 'E'
                ORDER BY
                    "Date"
            )
        SELECT
            Date,
            CASE
                WHEN Date < '2025-09-01' THEN '2025-09-01'
                ELSE Date
            END AS start_date,
            "Container Ref. No." AS container_number,
            "Line/Client" AS shipping_line,
                CASE
                WHEN "Date Out" > '2025-09-30' or "Date Out" IS NULL THEN '2025-09-30'
                ELSE "Date Out"
            END AS end_date,
            (end_date - Date) +1  AS days_in_storage,
            CASE WHEN days_in_storage > 10 THEN days_in_storage -10 
            	ELSE 0 
            END AS billable_days,
            "F/Vessel" AS remarks
        FROM
            month_storage
        """
    )
    return


@app.cell
def _(shifting_log_dataf):
    _df = mo.sql(
        f"""
        SELECT * FROM shifting_log_dataf WHERE "Date Shifted" BETWEEN '2025-09-01' AND '2025-09-30' --AND Client = 'SAPMER'
        """
    )
    return


@app.cell
def _(cold_store_dataf):
    by_catch = mo.sql(
        f"""
        SELECT
            day,
            date,
            CAST(SUM(total_tonnage) AS DECIMAL) AS total_tonnage,
            CAST(
                SUM(
                    CASE
                        WHEN overtime_tonnage = '' THEN 0
                        ELSE CAST(overtime_tonnage AS FLOAT8)
                    END
                ) AS DECIMAL
            ) AS overtime_tonnage

        FROM
            cold_store_dataf
        WHERE
            date BETWEEN '2025-09-01' AND '2025-09-30'
            AND customer = 'AMIRANTE'
            AND operation_type LIKE '%Unloading%'
        GROUP BY
            day,
            date
            -- total_tonnage
        ORDER BY
            date
        """
    )
    return (by_catch,)


@app.cell
def _(by_catch):
    _df = mo.sql(
        f"""
        SELECT SUM(total_tonnage),SUM(overtime_tonnage) FROM by_catch
        """
    )
    return


@app.cell
def _():
    mo.md(r"""
    ### Haulage
    """)
    return


@app.cell
def _():
    line_selector = mo.ui.dropdown(options=["MAERSK","CMA CGM","IOT"],value="IOT")
    line_selector
    return (line_selector,)


@app.cell
def _(line_selector, transfer_log_dataf):
    _df = mo.sql(
        f"""
        SELECT * FROM transfer_log_dataf WHERE Date BETWEEN '2025-09-01' AND '2025-11-30'
            AND "Line/Client" = '{line_selector.value}' --AND Status = 'Empty' --AND Driver <> 'Hunt Deltel'
        """
    )
    return


@app.cell
def _():
    (1234.484 - 192.348)*26.5
    return


@app.cell
def _(line_selector, transfer_log_dataf):
    _df = mo.sql(
        f"""
        SELECT
            "Driver",
            "Status",
            COUNT(*) AS number_of_records
        FROM
            transfer_log_dataf
        WHERE
            Date BETWEEN '2025-09-01' AND '2025-09-30'
            AND "Line/Client" = '{line_selector.value}'
        GROUP BY 
        	    "Driver",
            "Status"
        """
    )
    return


@app.cell
def _():
    container_operations_path = r"C:\Users\gmounac\Dropbox\Container and Transport\Container Section\Container Operations Activity\Container Operation Activity.xlsx"
    return (container_operations_path,)


@app.cell
def _(stdu_dataf):
    _df = mo.sql(
        f"""
        SELECT
            Date,
            "Movement Type",
            SUM("No of Scows") AS number_of_scows
        FROM
            stdu_dataf
        WHERE
            Status = 'Empty'
            AND "No of Scows" > 0
            AND DATE BETWEEN '2025-09-01' AND '2025-09-30'
        GROUP BY
            Date,
            "Movement Type"
        ORDER BY 
        	Date
        """
    )
    return


@app.cell
def _(transfer_dataf):
    _df = mo.sql(
        f"""
        SELECT
            *
        FROM
            transfer_dataf
        WHERE
            date BETWEEN '2025-09-01' AND '2025-10-31' --AND line = 'IOT';
        """
    )
    return


@app.cell
def _(stdu_dataf):
    _df = mo.sql(
        f"""
        SELECT * FROM stdu_dataf WHERE date > '2025-09-30' AND "No of Scows" > 0
        """
    )
    return


@app.cell
def _():
    (26.350 - 19.350) + 26.220
    return


@app.cell
def _(cold_store_dataf, stdu_dataf):
    _df = mo.sql(
        f"""
        WITH
            log_stdu AS (
                SELECT
                    Date,
                    SUM("No of Scows") AS num_scows
                FROM
                    stdu_dataf
                WHERE
                    date BETWEEN '2025-10-01' AND '2025-10-31'
                    AND "No of Scows" > 0
                    AND Status = 'Full'
                GROUP BY
                    "Date"
                ORDER BY
                    Date
            ),
            inv_stdu AS (
                SELECT
                    date,
                    customer,
                    ABS(total_tonnage) AS tonnage,
                    ABS(CAST(bins_out AS INTEGER)) AS scows
                FROM
                    cold_store_dataf
                WHERE
                    date BETWEEN '2025-10-01' AND '2025-10-31'
                    AND operation_type LIKE '%IOT%'
            )
        SELECT
            *
        FROM
            inv_stdu i
            FULL OUTER JOIN log_stdu l ON i.date = l.date
        ORDER BY i.date
        """
    )
    return


@app.cell
def _(cold_store_dataf):
    _df = mo.sql(
        f"""
        SELECT
            *,
            ABS(total_tonnage) AS tonnage,
            ABS(CAST(bins_out AS INTEGER)) AS scows
        FROM
            cold_store_dataf
        WHERE
            date BETWEEN '2025-10-01' AND '2025-10-31'
            AND operation_type LIKE '%IOT%'
        """
    )
    return


@app.cell
def _():
    pallet_dataf = GoogleSheetsLoader().load_sheet(config_name="PaLLET_AND_LINER").data
    return (pallet_dataf,)


@app.cell
def _(pallet_dataf):
    _df = mo.sql(
        f"""
        SELECT
            *
        FROM
            pallet_dataf
        WHERE
            date BETWEEN '2025-10-01' AND '2025-10-31' AND shipping_line = 'IOT'
        """
    )
    return


@app.cell
def _(netlist):
    _df = mo.sql(
        f"""
        SELECT
            *
        FROM
            netList
        WHERE
            date BETWEEN '2025-09-13' AND '2025-09-20'
            AND destination = 'MNBU3303690'
        """
    )
    return


@app.cell
def _(netlist):
    _df = mo.sql(
        f"""
        SELECT * FROM netList WHERE service IS NULL
        """
    )
    return


@app.cell
def _(electricity):
    _df = mo.sql(
        f"""
        SELECT * FROM electricity WHERE container_number = 'MNBU0252840'
        """
    )
    return


@app.cell
def _():
    6.210 * 80.625
    return


@app.cell
def _(electricity, netlist):
    _df = mo.sql(
        f"""
        WITH
            stuffing AS (
                SELECT
                    date,
                    destination,
                    ROUND(SUM(total_tonnage), 3) AS total_tonnage
                FROM
                    netList
                WHERE
                    date BETWEEN '2025-09-01' AND '2025-10-31'
                    AND vessel = 'ALAKRANA'
                    AND service LIKE '%Full%'
                GROUP BY
                    date,
                    destination
            ),
            pluggin AS (
                SELECT
                    vessel_client,
                    date_plugged,
                    container_number,
                    tonnage
                FROM
                    electricity
                WHERE
                    --shipping_line = 'IOT' -- AND vessel_client = 'GALERNA TRES'
                    -- AND 
                    date_plugged BETWEEN '2025-09-01' AND '2025-10-31'
            )
        SELECT
            p.date_plugged,
            s.destination,
            s.total_tonnage AS total_stuffed,
            p.tonnage AS total_plugged,
            (s.total_tonnage = p.tonnage) AS check
        FROM
            stuffing s
            JOIN pluggin p ON s.date = p.date_plugged
            AND s.destination = p.container_number
        """
    )
    return


@app.cell
def _():
    (10 - 3.79) == round((1263.321 - 1257.111),3)
    return


@app.cell
def _():
    mo.md(r"""
    MSFU0010048
    """)
    return


@app.cell
def _(ops_dataf):
    bernica = mo.sql(
        f"""
        SELECT
            *
        FROM
            ops_dataf
        WHERE "Container Ref. No." = 'CGMU5124861'
        -- WHERE
        --     "Date affected" BETWEEN '2025-08-29' AND '2025-09-04'
        --     AND F LIKE '%Bernica%' AND "Shipping Line" = 'Maersk' AND "Container Ref. No." <> 'SUDU6004445' AND Type = 'Stuffing'
        """
    )
    return


@app.cell
def _(electricity):
    _df = mo.sql(
        f"""
        SELECT * FROM electricity
        """
    )
    return


@app.cell
def _(electricity, ops_dataf):
    _df = mo.sql(
        f"""
        WITH
            log_stuffing AS (
                SELECT
                    F AS Vessel,
                    "Date affected" as date_affected,
                    "Container Ref. No." AS container_number,
                    CAST("Tonnage" * 0.001 AS DECIMAL) AS tonnage
                FROM
                    ops_dataf
                WHERE
                    "Date affected" BETWEEN '2025-09-01' AND '2025-09-30'
                    --AND "Shipping Line" = 'MAERSK'
            ),
            inv_stuffing AS (
                SELECT
                    vessel_client,
                    date_plugged,
                    container_number,
                    tonnage
                FROM
                    electricity
                WHERE
                   -- shipping_line = 'MAERSKLINE' AND 
            date_plugged BETWEEN '2025-09-01' AND '2025-09-30'
            )
        SELECT
            *,
            (i.tonnage = l.tonnage) AS diff
        FROM
            inv_stuffing i
            JOIN log_stuffing l ON i.date_plugged = l.date_affected
            AND i.container_number = l.container_number
        """
    )
    return


@app.cell
def _():
    (7 * 70) + 25
    return


@app.cell
def _():
    pti_dataf = GoogleSheetsLoader().load_sheet(config_name="CONTAINER_PTI_LOG").data
    cleaning_dataf = GoogleSheetsLoader().load_sheet(config_name="CONTAINER_CLEANING_LOG").data
    return (cleaning_dataf,)


@app.cell
def _():
    return


@app.cell
def _(cleaning_dataf):
    _df = mo.sql(
        f"""
        SELECT * FROM cleaning_dataf  WHERE
            date BETWEEN '2025-01-01' AND '2025-12-31' AND shipping_line = 'IOT'
        """
    )
    return


@app.cell
def _(cleaning_dataf, gatein_log):
    _df = mo.sql(
        f"""
        WITH
            washing AS (
                SELECT
                    *
                FROM
                    cleaning_dataf
                WHERE
                    date BETWEEN '2025-10-01' AND '2025-10-31'
                    AND shipping_line = 'MAERSK'
                    AND "Invoice To" = 'MAERSKLINE'
            ),
            gate AS (
                SELECT
                    Date,
                    Time,
                    "Container Number" AS container_number,
            "Shipping Line" AS shipping_line,
                    "PTI Status" AS pti_status
                FROM
                    gatein_log
                WHERE
                    date BETWEEN '2025-01-01' AND '2025-10-31'
            )
        SELECT
            *
        FROM
            washing w
            LEFT JOIN gate g ON g.container_number = w.container_number
            AND w.date >= g.date
        """
    )
    return


@app.cell
def _():


    electricity = (
        GoogleSheetsLoader().load_sheet(config_name="Electricity").data
    )
    return (electricity,)


@app.cell
def _(electricity):
    _df = mo.sql(
        f"""
        SELECT
            *
        FROM
            electricity WHERE time_plugged = TIME '00:00:00' AND YEAR(date_plugged) = 2025 AND operation_type NOT LIKE '%Direct%' AND MONTH(date_plugged) >= 10;
        """
    )
    return


@app.cell
def _(container_operations_path):
    ops_dataf = pl.read_excel(container_operations_path,sheet_name="Plug in Plug out")
    return (ops_dataf,)


@app.cell
def _(ops_dataf):
    ops_dataf
    return


@app.cell
def _():
    ctn = mo.ui.text(label="Container Number: ",max_length=11)
    ctn
    return (ctn,)


@app.cell
def _(ctn, ops_dataf):
    ops_df = mo.sql(
        f"""
        SELECT
            F,
            Shipper,
            "Date affected" as date_affected,
            CAST("Plugin Time" AS TIME) AS plugin_time,
            "Container Ref. No." AS container_ref_no,
            "Type",
            "Shipping Line" as shipping_line,
            "Status",
            ROUND(CAST(Tonnage * 0.001 AS FLOAT8), 3) AS Tonnage,
            "Date Out" as Date_Out,
            (Date_Out - date_affected) +1 AS days_on_plug
        FROM
            ops_dataf 
        WHERE container_ref_no = '{ctn.value}' AND YEAR(date_affected) = 2025 --AND MONTH(date_affected) = 10;
        """
    )
    return (ops_df,)


@app.cell
def _():
    (2*70) + 30 +25
    return


@app.cell
def _(ops_df):
    _df = mo.sql(
        f"""
        SELECT * FROM ops_df WHERE Vessel = 'Amirante' AND YEAR(date_affected) = 2025 AND MONTH(date_affected) = 10
            AND days_on_plug > 1
        """
    )
    return


@app.cell
def _():
    return


@app.cell
def _():
    plugin_log = (
        GoogleSheetsLoader().load_sheet(config_name="Container_Plugin_Log").data
    )
    gatein_log = (
        GoogleSheetsLoader().load_sheet(config_name="Container_Gate_In").data
    )

    # tmp = GoogleSheetsLoader().load_sheet(config_name="Container_Temperature_log").data
    return gatein_log, plugin_log


@app.cell
def _():
    cccs_stuffing = (
        GoogleSheetsLoader().load_sheet(config_name="CCCS_Container_Stuffing").data
    )
    return (cccs_stuffing,)


@app.cell
def _():
    selected_date = mo.ui.date()
    selected_date
    return (selected_date,)


@app.cell(hide_code=True)
def _(cccs_stuffing, gatein_log, netlist, plugin_log, selected_date):
    _df = mo.sql(
        f"""
        WITH
            plugin AS (
                SELECT
                    "Date" AS plugin_date,
                    "Time" AS plugin_time,
                    "Container Number" AS container_number,
                    "Set Point" AS set_point,
                    CASE
                        WHEN "Shipping Line" = 'MAERSK' THEN 'MAERSKLINE'
                        ELSE "Shipping Line"
                    END AS shipping_line
                FROM
                    plugin_log
            ),
            cold_store AS (
                SELECT
                    date,
                    customer AS vessel,
                    container_number AS destination,
                    TIME '08:00:00' AS end_time,
                    total_tonnage,
                    'Cold Store' AS service
                FROM
                    cccs_stuffing
                WHERE
                    YEAR(date) = 2025
            ),
            laden_gate_in AS (
                SELECT
                    "Date" AS date,
                    CASE
                        WHEN "Shipping Line" = 'MAERSK' THEN 'MAERSKLINE'
                        ELSE "Shipping Line"
                    END AS vessel,
                    "Container Number" AS destination,
                    "Time" as end_time,
                    0 AS total_tonnage,
                    'Plugin' AS Service
                FROM
                    gatein_log
                WHERE
                    Status <> 'Empty'
                    AND "Type" <> 'Dry'
                    AND YEAR(date) = 2025
            ),
            gate_in AS (
                SELECT
                    MAX("Date") AS date_in,
                    "Container Number" AS container_number,
                    LAST(CASE
                        WHEN "Shipping Line" = 'MAERSK' THEN 'MAERSKLINE'
                        ELSE "Shipping Line"
                    END) AS shipping_line,
                    LAST("Type") as container_type
                FROM
                    gatein_log
            GROUP BY 
            	"Container Number"

            ),
            stuffing AS (
                SELECT
                    date,
                    vessel,
                    -- MIN(start_time) as min_time,
                    destination,
                    -- overtime,
                    -- storage_type,
                    MAX(end_time) as end_time,
                    CAST(SUM(total_tonnage) AS DECIMAL) AS total_tonnage,
                    service
                FROM
                    netList
                GROUP BY
                    date,
                    vessel,
                    destination,
                    service
            ),
            combined AS (
                SELECT
                    *
                FROM
                    stuffing
                UNION ALL
                SELECT
                    *
                FROM
                    cold_store
                UNION ALL
                SELECT
                    *
                FROM
                    laden_gate_in
            )

        -- SELECT * FROM combined c  JOIN plugin p ON c.date = p.plugin_date
        --     AND c.destination = p.container_number
        --     JOIN gate_in g ON c.destination = g.container_number AND c.date >= g.date_in

        -- WHERE date = '2025-11-17'


        SELECT
            s.date,
            s.vessel,
            s.destination,
            -- s.overtime,
            s.end_time,
            s.total_tonnage,
            s.service,
            p.plugin_time,
            p.set_point,
            p.shipping_line,
            g.container_type,
            (p.plugin_date = s.date) AS check_date
        FROM
            combined s
            LEFT JOIN plugin p ON s.destination = p.container_number  AND p.plugin_date = s.date

            JOIN gate_in g ON g.container_number = s.destination AND s.date >= g.date_in
        WHERE
            s.date = '{selected_date.value}'
            AND (
                service NOT IN (
                    'Transhipment',
                    'Unload to Quay',
                    'Unload to CCCS'
                )
                OR service IS NULL
            )
        ORDER BY
            s.vessel,
            p.plugin_time ASC;
        """
    )
    return


@app.cell
def _():
    pti_log_dataf = GoogleSheetsLoader().load_sheet(config_name="CONTAINER_PTI_LOG").data
    pti_inv_dataf = GoogleSheetsLoader().load_sheet(config_name="CONTAINER_PTI").data
    return pti_inv_dataf, pti_log_dataf


@app.cell
def _(pti_log_dataf):
    pti_log_dataf.collect()
    return


@app.cell
def _(pti_log_dataf):
    _df = mo.sql(
        f"""
        SELECT
            *
        FROM
            pti_log_dataf
        WHERE
            "Date Plugin" BETWEEN '2025-10-01' AND '2025-10-31' AND "Shipping Line" = 'IOT'
        """
    )
    return


@app.cell
def _(pti_inv_dataf, pti_log_dataf):
    _df = mo.sql(
        f"""
        WITH
            invoice AS (
                SELECT
                    *
                FROM
                    pti_inv_dataf
                WHERE
                    datetime_start BETWEEN '2025-11-01' AND '2025-11-30' AND invoice_to = 'MAERSKLINE'
            ),
            logistics AS (
                SELECT
                    "Date Plugin" AS date_plugin,
                    "Time Plugin" AS time_plugin,
            MAKE_TIMESTAMP(
                    EXTRACT (
                        YEAR
                        FROM
                            date_plugin
                    ),
                    EXTRACT (
                        MONTH
                        FROM
                            date_plugin
                    ),
                    EXTRACT (
                        DAY
                        FROM
                            date_plugin
                    ),
                    EXTRACT (
                        HOUR
                        FROM
                            time_plugin
                    ),
                    EXTRACT (
                        MINUTE
                        FROM
                            time_plugin
                    ),
                    EXTRACT (
                        SECOND
                        FROM
                            time_plugin
                    )
                ) AS datetime_start,
                    "Container Number" AS container_number,
                    Size,
                    "Set Point",
                    "Unit Manufacturer",
                    "Shipping Line" AS shipping_line,
                  --  "Unplugged Date" AS unplugged_date,
                    CASE
                        WHEN "Unplugged Date" = 'On Plug' THEN NULL
                        ELSE CAST(
                            STRPTIME("Unplugged Date", '%d/%m/%Y %H:%M') AS DATETIME
                        )
                    END AS unplugged_date,
                    "Sticker",
                    "Status",
            	"Generator"
                FROM
                    pti_log_dataf
                WHERE
                   -- date_plugin
            datetime_start BETWEEN '2025-11-01' AND '2025-11-30'
                    AND shipping_line = 'MAERSK'
            )
        SELECT
            *
        FROM
            logistics l
            FULL OUTER JOIN invoice i ON i.container_number = l.container_number
            AND i.datetime_end = l.unplugged_date AND l.datetime_start = i.datetime_start
        """
    )
    return


@app.cell
def _():
    _df = mo.sql(
        f"""
        --CREATE TYPE line AS ENUM ('MAERSK','CMA CGM','IOT');
        """
    )
    return


@app.cell
def _(gatein_log, pti_log_dataf):
    pti_view = mo.sql(
        f"""
        WITH
            gate_type AS (
                SELECT
                    "Container Number" AS container_number,
                    "Shipping Line" AS shipping_line,
                    "Type" AS cnt_type,
                    "Unit Manufacturer" AS cnt_man
                FROM
                    gatein_log
            ),
            pre_trip AS (
                SELECT
                    -- CASE
                    --     WHEN "Date Plugin" = '' THEN NULL
                    --     ELSE CAST(STRPTIME("Date Plugin", '%d/%m/%Y') AS DATE)
                    -- END AS date_plugin,
                    -- CASE
                    --     WHEN "Time Plugin" = '' THEN NULL
                    --     ELSE CAST(STRPTIME("Time Plugin", '%H:%M:%S') AS TIME)
                    -- END AS time_plugin,
                    "Date Plugin" AS date_plugin,
                    "Time Plugin" AS time_plugin,
                    "Container Number" AS container_number,
                    Size,
                    "Set Point",
                    "Unit Manufacturer",
                    "Shipping Line"  AS shipping_line,
                    -- "Unplugged Date",
                    CASE
                        WHEN "Unplugged Date" = 'On Plug' THEN NULL
                        ELSE CAST(
                            STRPTIME("Unplugged Date", '%d/%m/%Y %H:%M') AS DATETIME
                        )
                    END AS unplugged_date,
                    "Sticker",
                    "Status"
                FROM
                    pti_log_dataf
                WHERE
                    date_plugin BETWEEN '2025-11-01' AND '2025-11-30'
                    --AND shipping_line = 'MAERSK'
            )
        SELECT
            p.*,
            -- g.cnt_type,
            g.cnt_man,
            g.shipping_line,
            ("Unit Manufacturer" = g.cnt_man) AS check_manufacturer,
            (
                unplugged_date - MAKE_TIMESTAMP(
                    EXTRACT (
                        YEAR
                        FROM
                            date_plugin
                    ),
                    EXTRACT (
                        MONTH
                        FROM
                            date_plugin
                    ),
                    EXTRACT (
                        DAY
                        FROM
                            date_plugin
                    ),
                    EXTRACT (
                        HOUR
                        FROM
                            time_plugin
                    ),
                    EXTRACT (
                        MINUTE
                        FROM
                            time_plugin
                    ),
                    EXTRACT (
                        SECOND
                        FROM
                            time_plugin
                    )
                )
            ) AS duration_on_plug
        FROM
            pre_trip p
            LEFT JOIN gate_type g ON p.container_number = g.container_number
        --WHERE duration_on_plug < '00:00'::INTERVAL
        """
    )
    return (pti_view,)


@app.cell
def _(pti_view):
    _df = mo.sql(
        f"""
        SELECT * FROM pti_view ORDER BY container_number ASC
        """
    )
    return


@app.cell
def _(pti_view):

    pl.from_pandas(pti_view).filter(pl.col("container_number").is_unique().not_()).sort(by='date_plugin')
    return


@app.cell
def _():
    ops_activity_path = r"C:\Users\gmounac\Dropbox\! OPERATION SUPPORTING DOCUMENTATION\2025\2025 IPHS operation activity.xlsx"

    ops_act_dataf = pl.read_excel(ops_activity_path)

    extra_men_check_dataf = pl.read_excel(ops_activity_path,sheet_name="Extramen")
    return extra_men_check_dataf, ops_act_dataf


@app.cell
def _(ops_act_dataf):
    ops = mo.sql(
        f"""
        SELECT
            *,
            CASE 
            	WHEN "Number of Stevedores" < 47 THEN 0 ELSE ("Number of Stevedores" - 47) END AS checking
        FROM
            ops_act_dataf
        WHERE
            DATE BETWEEN '2025-09-21' AND '2025-10-30'
        AND "VESSEL NAME" = 'ALBACAN'
        """
    )
    return (ops,)


@app.cell
def _(extra_men_check_dataf, ops):
    _df = mo.sql(
        f"""
        WITH main AS (SELECT
            DAY,
            DATE,
            "VESSEL NAME",
            ROUND("TOTAL TONNAGE", 3) AS total_tonnage,
            -- "Transhipment Brine" AS trans_brine,
            -- "Simple Unloading Brine" AS simpl_brine,
            -- "Unloading to CCCS Brine" AS unl_cccs_brine,
            "Extra Men" AS extra_men,

            "Number of Stevedores" AS num_stevedores,
            "Comments",
            checking
        FROM
            ops),
            extra AS (SELECT
            *
        FROM
            extra_men_check_dataf
        WHERE
          NOT  "Check" AND MONTH(Date)>=9)


        SELECT e.date,e.vessel,m.extra_men,e.comments,e.num_as_per_comments,m.num_stevedores FROM extra e JOIN main m ON m.date = e.date
        """
    )
    return


@app.cell
def _(ops):
    ios = mo.sql(
        f"""
        SELECT
            DAY,
            DATE,
            "VESSEL NAME",
            ROUND("TOTAL TONNAGE", 3) AS total_tonnage,
            -- "Transhipment Brine" AS trans_brine,
            -- "Simple Unloading Brine" AS simpl_brine,
            -- "Unloading to CCCS Brine" AS unl_cccs_brine,
            "Extra Men" AS extra_men,
                "Overtime Tonnage" AS overtime_tonnage,
            "Number of Stevedores" AS num_stevedores,
            "Comments",
            checking
        FROM
            ops
        """
    )
    return (ios,)


@app.cell
def _(ios):
    _df = mo.sql(
        f"""
        SELECT DATE,Comments FROM ios
        """
    )
    return


@app.cell
def _(netlist):
    _df = mo.sql(
        f"""
        SELECT * FROM netList WHERE vessel = 'ALBACAN'
        """
    )
    return


@app.cell
def _():
    round(23.99 +1.43+ 20.65 +8.759,3)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
