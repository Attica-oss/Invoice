import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")

with app.setup:
    import polars as pl
    import openpyxl
    import marimo as mo


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Transfer of Scows
    """)
    return


@app.cell
def _():
    from dataframe import full_scows,empty_scows

    return empty_scows, full_scows


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Full Scow Transfer

    *  Has the Trucking price of $3.5/tons,
    *  Has the CCCS movement fee of $3.5/tons
    """)
    return


@app.cell(hide_code=True)
def _(full_scows):
    full_scow_transfer_df = mo.sql(
        f"""
        FROM
            full_scows
        SELECT
            day_name,
            date,
            customer,
            CASE
                WHEN movement_type LIKE '%Delivery%' THEN 'Delivery'
                WHEN movement_type LIKE '%Collection%' THEN 'Collection'
                ELSE 'Invalid Move'
            END AS movement_type,
            overtime,
            num_of_scows AS number_of_scows,
            tonnage,
            storage_type,
            Price AS unit_price,
            total_price AS movement_fee,
            total_price AS scow_transfer_price,
            'FULL SCOW TRANSFER' AS service,
            'BIN DISPATCH TO/FROM IOT' AS sub_type,
        'MONTHLY' AS report_type
        """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Empty Scow Transfer

    *  Has the Trucking price of $3.5/scows,
    """)
    return


@app.cell
def _(empty_scows):
    empty_scow_transfer_df = mo.sql(
        f"""
        FROM
            empty_scows
        SELECT
            day_name,
            date,
            customer,
            CASE
                WHEN movement_type LIKE '%Delivery%' THEN 'Delivery'
                WHEN movement_type LIKE '%Collection%' THEN 'Collection'
                ELSE 'Invalid Move'
            END AS movement_type,
            overtime,
            num_of_scows AS number_of_scows,
            -- tonnage,
            -- storage_type,
            Price AS unit_price,
            -- total_price AS movement_fee,
            total_price AS scow_transfer_price,
            'EMPTY SCOW TRANSFER' AS service,
            'MONTHLY' AS sub_type,
        'MONTHLY' AS report_type
        """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Forklift Scow Handling

    * For both empty and full scow handling
    """)
    return


@app.cell(hide_code=True)
def _(empty_scows, full_scows):
    forklift_scow_handling = mo.sql(
        f"""
        WITH full_ AS (FROM
            full_scows
        SELECT
            day_name,
            date,
            customer,
            CASE
                WHEN movement_type LIKE '%Delivery%' THEN 'Loading Full Scows'
                WHEN movement_type LIKE '%Collection%' THEN 'Unloading Full Scows'
                ELSE 'Invalid Move'
            END AS forklift_service,
            overtime,
            num_of_scows AS number_of_scows,
            -- tonnage,
            -- storage_type,
            6 AS unit_price,
            CASE
                WHEN overtime = 'overtime 200%' THEN unit_price * num_of_scows * 2.0
                WHEN overtime = 'overtime 150%' THEN unit_price * num_of_scows * 1.5
                WHEN overtime = 'normal hours' THEN unit_price * num_of_scows * 1.0
                ELSE 0
            END AS forklift_price,
            'FORKLIFT SCOW HANDLING' AS service,
            'BIN DISPATCH TO/FROM IOT' AS sub_type,
            'MONTHLY' AS report_type), 
            empty_ AS (FROM
            empty_scows
        SELECT
            day_name,
            date,
            customer,
            CASE
                WHEN movement_type LIKE '%Delivery%' THEN 'Loading Empty Scows'
                WHEN movement_type LIKE '%Collection%' THEN 'Unloading Empty Scows'
                ELSE 'Invalid Move'
            END AS forklift_service,
            overtime,
            num_of_scows AS number_of_scows,
            -- tonnage,
            -- storage_type,
            6 AS unit_price,
            CASE
                WHEN overtime = 'overtime 200%' THEN unit_price * num_of_scows * 2.0
                WHEN overtime = 'overtime 150%' THEN unit_price * num_of_scows * 1.5
                WHEN overtime = 'normal hours' THEN unit_price * num_of_scows * 1.0
                ELSE 0
            END AS forklift_price,
            'FORKLIFT SCOW HANDLING' AS service,
            'MONTHLY' AS sub_type,
            'MONTHLY' AS report_type)

        FROM full_ 
        UNION ALL
        FROM empty_

        ORDER BY date
        """
    )
    return


@app.cell
def _():
    from dataframe import washing,pti,shifting

    return pti, shifting, washing


@app.cell(hide_code=True)
def _(washing):
    washing_df = mo.sql(
        f"""
        WITH
            data_ AS (
                FROM
                    washing
                SELECT
                    *,
                    CASE
                        WHEN invoice_to IN ('MAERSKLINE', 'CMA CGM') THEN 'MONTHLY'
                        WHEN invoice_to = 'IOT' THEN 'M&R'
                        WHEN invoice_to IN ('INVALID', 'IPHS') THEN 'NOT TO INVOICE'
                        WHEN invoice_to NOT IN ('MAERSKLINE', 'CMA CGM', 'IOT')
                        AND service_remarks = 'Unstuffed' THEN 'CROSS STUFFING SI'
                        ELSE 'SI'
                    END AS sub_type
            ),
            grouped_data_ AS (
                FROM
                    data_
                SELECT
                    * EXCLUDE (price, container_number, service_remarks),
                    SUM(price) AS washing_price,
                    COUNT(*) AS number_of_service
                GROUP BY ALL
                ORDER BY
                    date
            )
        FROM
            grouped_data_
        SELECT
           STRFTIME(date,'%a') AS day_name,
            date,
            invoice_to AS customer,
            number_of_service,
            30 AS unit_price,
            washing_price,
            'CONTAINER WASHING' AS service,
            sub_type,
            CASE
                WHEN sub_type IN ('MONTHLY', 'M&R') THEN 'MONTHLY'
                WHEN sub_type = 'NOT TO INVOICE' THEN 'NOT TO INVOICE'
                ELSE 'SI'
            END AS report_type
        """
    )
    return


@app.cell
def _(pti):
    pti_df = mo.sql(
        f"""
        WITH
            data_ AS (
                FROM
                    pti
                SELECT
                    STRFTIME(datetime_start, '%a') AS day_name,
                    datetime_start::DATE AS date,
                    CASE
                        WHEN set_point = -25 THEN 'STANDARD'
                        WHEN set_point = -35 THEN 'MAGNUM'
                        WHEN set_point = -60 THEN 'S FREEZER'
                        ELSE 'INVALID'
                    END AS container_type,
                    invoice_to AS customer,
                    CASE
                        WHEN "hours" > 8 THEN 'ABOVE'
                        ELSE 'BELOW'
                    END AS duration,
                    status,
                    CASE
                        WHEN no_shifting IS TRUE THEN 1
                        ELSE 0
                    END AS count_shifting,
                    plugin_price,
                    electricity_price,
                    shifting_price,
                    total_price,
                    'PTI' AS service,
                    CASE
                        WHEN customer = 'IOT' THEN 'M&R'
                        WHEN customer IN ('MAERSKLINE', 'CMA CGM') THEN 'MONTHLY'
                        ELSE 'NOT TO INVOICE'
                    END AS sub_type,
                    'MONTHLY' AS report_type
                WHERE
                    YEAR(datetime_start) = 2026
            )
        FROM
            data_
        SELECT
            day_name,
            date,
            container_type,
            customer,
            duration,
            status,
            COUNT(*) AS number_of_service,
            SUM(count_shifting) AS number_of_shifts,
            SUM(plugin_price) AS plugin_price,
            SUM(electricity_price) AS electricity_price,
            SUM(shifting_price) AS shifting_price,
            SUM(total_price) AS total_pti_price,
            service,
            sub_type,
            report_type
        GROUP BY ALL
        ORDER BY
            date
        """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Shifting
    """)
    return


@app.cell(hide_code=True)
def _(cross_stuffing, washing):
    _df = mo.sql(
        f"""
        WITH
            washing_ AS (
                FROM
                    washing
                SELECT
                    *
                WHERE
                    invoice_to NOT IN ('MAERSKLINE', 'CMA CGM', 'INVALID', 'IPHS')
            ),
            cross_ AS (
                FROM
                    cross_stuffing
                SELECT
                    origin AS container_number,
                    * EXCLUDE (origin)
                WHERE
                    is_origin_empty = TRUE
                    AND invoiced NOT IN ('MAERSKLINE', 'CMA CGM', 'INVALID', 'IPHS')
            ),
            combined_data AS (
                SELECT
                    c.day AS day_name,
                    w.date,
                    w.invoice_to AS customer,
                    'Shifting at Washing' AS operation_type,
                    'CROSS STUFFING' AS service,
                       CASE
                WHEN c.day IN ('Sun', 'PH') THEN 35 * 1.5
                ELSE 35
            END AS shifting_price,
                    'SI' AS sub_type,
                    'SI' AS report_type
                FROM
                    cross_ c
                    LEFT JOIN washing_ w ON c.date <= w.date
                    AND w.container_number = c.container_number
                WHERE
                    w.date IS NOT NULL
            )
        FROM
            combined_data
        SELECT
            day_name,
            date,
            customer,
            operation_type,
            COUNT(*) AS number_of_service,
        SUM(shifting_price) AS shifting_price,
            service,
            sub_type,
            report_type
        GROUP BY ALL
        ORDER BY date
        """
    )
    return


@app.cell(hide_code=True)
def _(shifting):
    internal_shifting_df = mo.sql(
        f"""
        WITH data_ AS (FROM
            shifting
        SELECT
            day_name,
            date,
            invoice_to AS customer,
            CASE
                WHEN CONTAINS(service_remarks, 'OT')
                AND day_name IN ('Sun', 'PH') THEN price * 2.0
                WHEN CONTAINS(service_remarks, 'OT')
                AND day_name NOT IN ('Sun', 'PH') THEN price * 1.5
                ELSE price
            END AS price,
            'INTERNAL SHIFTING' AS service,
            CASE
                WHEN invoice_to IN (
                    'MAERSKLINE',
                    'CMA CGM',
                    'IOT',
                    'CCCS',
                    'ISLAND CATCH'
                ) THEN 'MONTHLY'
                ELSE 'SI'
            END AS sub_type,
            sub_type AS report_type
        WHERE
            YEAR(date) = 2026)

        FROM data_ 
        SELECT day_name,date,customer,COUNT(*) AS number_of_service,SUM(price) AS shifting_price,service,sub_type,report_type
        GROUP BY ALL
        ORDER BY date
        """
    )
    return


@app.cell
def _():
    from dataframe import dispatch_to_cargo,from_cccs_to_vessel,cross_stuffing,by_catch,cccs_stuffing

    return (
        by_catch,
        cccs_stuffing,
        cross_stuffing,
        dispatch_to_cargo,
        from_cccs_to_vessel,
    )


@app.cell(hide_code=True)
def _(dispatch_to_cargo):
    _df = mo.sql(
        f"""
        FROM
            dispatch_to_cargo
        SELECT
            day_name,
            date,
            vessel,
            customer,
            operation_type,
            total_tonnage,
            overtime_tonnage,
            CASE
                WHEN day_name IN ('Sun', 'PH') THEN CAST(
                    (
                        (total_tonnage - overtime_tonnage) * truck_price * 1.5
                    ) + (overtime_tonnage * truck_price * 2.0)
                 AS DECIMAL)
                ELSE CAST(
                    (
                        (total_tonnage - overtime_tonnage) * truck_price * 1.0
                    ) + (overtime_tonnage * truck_price * 1.5)
                AS DECIMAL)
            END AS truck_price,
            CASE
                WHEN day_name IN ('Sun', 'PH') THEN CAST(
                    (
                        (total_tonnage - overtime_tonnage) * cccs_movement_fee * 1.5
                    ) + (overtime_tonnage * cccs_movement_fee * 2.0)
               AS DECIMAL )
                ELSE CAST(
                    (
                        (total_tonnage - overtime_tonnage) * cccs_movement_fee * 1.0
                    ) + (overtime_tonnage * cccs_movement_fee * 1.5)
                AS DECIMAL)
            END AS cccs_movement_fee,
            CASE
                WHEN day_name IN ('Sun', 'PH') THEN CAST(
                    (
                        (total_tonnage - overtime_tonnage) * stevedores_on_cargo_fee * 1.5
                    ) + (overtime_tonnage * stevedores_on_cargo_fee * 2.0)
              AS DECIMAL  )
                ELSE CAST(
                    (
                        (total_tonnage - overtime_tonnage) * stevedores_on_cargo_fee * 1.0
                    ) + (overtime_tonnage * stevedores_on_cargo_fee * 1.5)
               AS DECIMAL )
            END AS stevedores_on_cargo_fee,
        CAST(total_price AS DECIMAL) AS total_dispatch_price,
        storage_type,
        'DISPATCH TO/FROM CARGO VESSEL' AS service,
        'SI' AS sub_type,
        'SI' AS report_type
        """
    )
    return


@app.cell(hide_code=True)
def _(from_cccs_to_vessel):
    _df = mo.sql(
        f"""
        FROM
            from_cccs_to_vessel
        SELECT
            "day" AS day_name,
            date,
            vessel,
            customer,
            'From Cold Store To Vessel' AS operation_type,
            total_tonnage,
            overtime_tonnage,
        'Dry' AS storage_type,
         CASE
                WHEN day_name IN ('Sun', 'PH') THEN CAST(
                    (
                        (total_tonnage - overtime_tonnage) * 7.5 * 1.5
                    ) + (overtime_tonnage * 7.5 * 2.0)
                 AS DECIMAL)
                ELSE CAST(
                    (
                        (total_tonnage - overtime_tonnage) * 7.5 * 1.0
                    ) + (overtime_tonnage * 7.5 * 1.5)
                AS DECIMAL)
            END AS truck_price,
            CASE
                WHEN day_name IN ('Sun', 'PH') THEN CAST(
                    (
                        (total_tonnage - overtime_tonnage) * 3.5 * 1.5
                    ) + (overtime_tonnage * 3.5 * 2.0)
               AS DECIMAL )
                ELSE CAST(
                    (
                        (total_tonnage - overtime_tonnage) * 3.5 * 1.0
                    ) + (overtime_tonnage * 3.5 * 1.5)
                AS DECIMAL)
            END AS cccs_movement_fee,
        total_price,
        'DISPATCH TO/FROM VESSEL' AS service,
        'SI' AS sub_type,
        'SI' AS report_type
        """
    )
    return


@app.cell(hide_code=True)
def _(cross_stuffing):
    _df = mo.sql(
        f"""
        FROM
            cross_stuffing
        SELECT
            "day" AS day_name,
            date,
            vessel_client AS vessel,
            invoiced AS customer,
            Service AS operation_type,
            total_tonnage,
            overtime_tonnage,
            Price AS unit_price,
            total_price,
        'CROSS STUFFING' AS service,
        CASE 
        	WHEN invoiced IN ('MAERSKLINE','IOT','CMA CGM') THEN 'MONTHLY' WHEN invoiced IN ('INVALID','IPHS') THEN 'NOT TO INVOICE' ELSE 'SI' END AS sub_type,
        sub_type AS report_type
        """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Extra Cross Stuffing Services

    * Tally
    * Tare Rental
    """)
    return


@app.cell
def _():
    from scan_google_sheet import scan_google_sheet

    return (scan_google_sheet,)


@app.cell
def _():
    misc_url = "https://docs.google.com/spreadsheets/d/1VbfiiWsp8yxs6KSR1CXpw1S_35tYlWV8UjjWah9Afpw/edit?pli=1&gid=757201340#gid=757201340"

    cross_stuffing_tab= "CrossStuffing"


    return cross_stuffing_tab, misc_url


@app.cell
def _(cross_stuffing_tab, misc_url, scan_google_sheet):
    extra_cross_stuffing_service = scan_google_sheet(url=misc_url,sheet_name=cross_stuffing_tab).filter(pl.col("date").dt.year().eq(2026))
    return (extra_cross_stuffing_service,)


@app.cell(hide_code=True)
def _(extra_cross_stuffing_service):
    _df = mo.sql(
        f"""
        FROM extra_cross_stuffing_service
        SELECT "day" AS day_name,date,vessel_client AS vessel,invoiced AS customer,total_tonnage,overtime_tonnage,tally
        WHERE service <> 'Unstuffing to CCCS' AND tally <> 'NA'
        """
    )
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(by_catch):
    _df = mo.sql(
        f"""
        WITH
            data_ AS (
                FROM
                    by_catch
                SELECT
                    "day" AS day_name,
                    date,
                    vessel,
                    customer,
                    operation_type,
                    total_tonnage,
                    overtime_tonnage,
                    Price AS unit_price,
                    CAST(total_price AS DECIMAL) AS total_price,
                    storage_type,
                    CASE
                        WHEN operation_type = 'CCCS (By-Catch)' THEN 'BY CATCH HANDLING'
                        ELSE 'TRANSFER OF BY CATCH'
                    END AS service,
                    'MONTHLY' AS sub_type,
                    'MONTHLY' AS report_type
            )
        FROM
            data_
        SELECT
            day_name,
            date,
            customer,
            COUNT(*) AS number_of_service,
            CAST(SUM(total_tonnage) AS DECIMAL) AS total_tonnage,
            SUM(overtime_tonnage) AS overtime_tonnage,
            unit_price,
            SUM(total_price) AS total_price,
            service,
            sub_type,
            report_type
        GROUP BY ALL
        ORDER BY
            date
        """
    )
    return


@app.cell(hide_code=True)
def _(cccs_stuffing):
    _df = mo.sql(
        f"""
        FROM
            cccs_stuffing
        SELECT
            day_name,
            date,
            customer AS vessel,
            invoiced AS customer,
            COUNT(*) AS number_of_container,
            Service AS operation_type,
            CAST(SUM(total_tonnage) AS DECIMAL) AS total_tonnage,
            SUM(overtime_tonnage) AS overtime_tonnage,
            Price AS unit_price,
            storage_type,
            CAST(SUM(total_price) AS DECIMAL) AS total_price,
            'COLD STORE STUFFING' AS service,
            CASE WHEN invoiced = 'MAERSKLINE' THEN 'CCCS OSS' ELSE 'SI' END AS sub_type,
            'OSS' AS report_type
        GROUP BY ALL
        ORDER BY
            date
        """
    )
    return


@app.cell
def _():
    from dataframe import oss,netList,iot_stuffing

    return iot_stuffing, oss


@app.cell(hide_code=True)
def _(iot_stuffing):
    _df = mo.sql(
        f"""
        WITH data_ AS (FROM
            iot_stuffing
        SELECT
            day_name,
            date,
            vessel,
            'IOT' AS customer,
            overtime,
            total_tonnage,
            Price AS unit_price,
            "storage" AS storage_type,
            CAST(total_price AS DECIMAL) AS total_price) 

        FROM data_ 
        SELECT day_name,date,vessel,customer,overtime,count(*) AS number_of_containers,CAST(SUM(total_tonnage) AS DECIMAL) AS total_tonnage,unit_price,SUM(total_price) AS total_price,
        'CONTAINER STUFFING' AS service,'MONTHLY' AS sub_type,'MONTHLY' AS report_type

        GROUP BY ALL
        ORDER BY date
        """
    )
    return


@app.cell(hide_code=True)
def _(oss):
    _df = mo.sql(
        f"""
        FROM oss
        SELECT 
         STRFTIME(date,'%a') AS day_name,
        date,
        vessel,
            'MAERSKLINE' AS customer,
            COUNT(*) AS number_of_containers,
        overtime,
            storage_type,
        CAST(SUM(total_tonnage) AS DECIMAL) AS total_tonnage,
        service AS operation_type,
        Price AS unit_price,
        CAST(SUM(invoice_value) AS DECIMAL) AS total_price,
            'CONTAINER STUFFING' AS service,
        UPPER(operation_type) AS sub_type,
        'OSS' AS report_type,
        GROUP BY ALL
        ORDER BY date


        """
    )
    return


@app.cell(hide_code=True)
def _(netlist):
    _df = mo.sql(
        f"""
        FROM netList
            SELECT STRFTIME(date,'%a') AS day_name,
            date,
            CASE WHEN remarks IS NULL THEN vessel ELSE remarks || ' // EX-' ||vessel END AS vessel,
            service AS operation_type,
            overtime,
            CAST(SUM(total_tonnage) AS DECIMAL) AS total_tonnage,
            storage_type,
            Price AS unit_price,
            CAST(SUM(invoice_value) AS DECIMAL) AS total_price,
            'UNLOADING VESSEL' AS service,
            'STO' AS sub_type,
            'STO' AS report_type
        WHERE YEAR(date) = 2026
        GROUP BY ALL
        ORDER BY date
        """
    )
    return


@app.cell
def _():
    from dataframe import salt,forklift_for_salt

    return forklift_for_salt, salt


@app.cell(hide_code=True)
def _(salt):
    _df = mo.sql(
        f"""
        FROM salt
            SELECT day_name,date,vessel,customer,operation_type,SUM(tonnage) AS total_tonnage,SUM(normal) AS normal_hour_tonnage,SUM(overtime_150) AS overtime_150_tonnage,SUM(overtime_200) AS overtime_200_tonnage, CAST(SUM(price) AS DECIMAL) AS total_price,
            'SALT OPERATION' AS service,
            CASE WHEN customer IN ('HARTSWATER LIMITED','ECHEBASTAR') THEN 'MONTHLY' ELSE 'STO' END AS sub_type,
            sub_type AS report_type
        WHERE YEAR(date) = 2026
        GROUP BY ALL
        ORDER BY date
        """
    )
    return


@app.cell(hide_code=True)
def _(forklift_for_salt):
    _df = mo.sql(
        f"""
        FROM forklift_for_salt
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
