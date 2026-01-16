import marimo

__generated_with = "0.19.4"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells
    import polars as pl
    import marimo as mo
    from dataframe.netlist import netList
    from utils.google_sheet import GoogleSheetsLoader


@app.cell
def _():
    gs = GoogleSheetsLoader()
    return (gs,)


@app.cell
def _(gs):
    report = gs.load_sheet(config_name="validation", parse_dates=True).data
    return (report,)


@app.cell
def _(gs):
    elec_log = gs.load_sheet(config_name="CONTAINER_PLUGIN_LOG").data
    return (elec_log,)


@app.cell
def _(netlist):
    _df = mo.sql(
        f"""
        SELECT * FROM netList
        """
    )
    return


@app.cell
def _(elec_log):
    _df = mo.sql(
        f"""
        SELECT * FROM elec_log WHERE Date > '2025-12-01'
        """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## STO Reports
    """)
    return


@app.cell
def _():
    month_selector = mo.ui.dropdown(options=['January','February','March','April','May','June','July','August','September','October','November','December'])

    month_selector
    return


@app.cell
def _(report):
    sto_dataf = mo.sql(
        f"""
        --- STO reports for December 25'
        SELECT
            report_name AS vessel,
            sub_type AS id,
            customer,
            starting_date,
            STRPTIME(ending_date, '%d/%m/%Y')::DATE AS ending_date
        FROM
            report
        WHERE
            report_type = 'sto' 
            AND "month" LIKE '%December%'
        ORDER BY
            starting_date ASC
        """,
        output=False
    )
    return (sto_dataf,)


@app.cell
def _(netlist, sto_dataf):
    net_dataf = mo.sql(
        f"""
        WITH
            sto AS (
                SELECT
                    *
                FROM
                    sto_dataf
            ),
            net AS (
                SELECT
                    *
                FROM
                    netList
            ),
        joined AS (


        SELECT
            s.id,
            n.date,
            n.vessel,
            n.service,
            n.total_tonnage,
            n.invoice_value

        FROM
            sto s
            JOIN net n ON n.date BETWEEN s.starting_date AND s.ending_date
            AND n.vessel = s.vessel
        WHERE YEAR(n.date) = 2025
        )

        SELECT id,vessel,ROUND(SUM(total_tonnage),3) AS tonnage, ROUND(SUM(invoice_value),3) AS total FROM joined GROUP BY id,vessel
        """
    )
    return (net_dataf,)


@app.cell
def _():
    from dataframe.stuffing import coa
    return (coa,)


@app.cell
def _(coa):
    electricity = coa
    return (electricity,)


@app.cell
def _(electricity, sto_dataf):
    electricity_dataf = mo.sql(
        f"""
        WITH
            sto AS (
                SELECT
                    *
                FROM
                    sto_dataf
            ),
            elect AS (
                SELECT
                    *
                FROM
                    electricity
            ),
            joined AS (
                SELECT
                    s.id,
                    e.vessel_client AS vessel,
                    e.container_number,
                    e.date_out,
                    e.plugin_price,
                    e.monitoring_price,
                    e.total_electricity,
                    e.total
                FROM
                    sto s
                    JOIN elect e ON e.date_plugged BETWEEN s.starting_date AND s.ending_date
                    AND s.vessel = e.vessel_client
                WHERE
                    e.customer NOT IN ('MAERSKLINE', 'IOT')
            )
        SELECT
            id,
            vessel,
            SUM(plugin_price) AS plugin_price,
            SUM(monitoring_price) AS monitoring_price,
            SUM(total_electricity) AS electricity_price,
            SUM(total) AS total
        FROM
            joined
        GROUP BY
            id,
            vessel
        """
    )
    return (electricity_dataf,)


@app.cell
def _():
    from data_source.make_dataset import load_gsheet_data
    from data_source.sheet_ids import (
        # MISC_SHEET_ID,
        OPS_SHEET_ID,
        raw_sheet,
        net_list_sheet,
        # by_catch_sheet,
        # all_cccs_data_sheet,
    )


    raw_ops_dataf = load_gsheet_data(OPS_SHEET_ID, raw_sheet)
    return (raw_ops_dataf,)


@app.cell
def _(raw_ops_dataf, sto_dataf):
    tare_dataf = mo.sql(
        f"""
        WITH
            sto AS (
                SELECT
                    *
                FROM
                    sto_dataf
            ),
            tare AS (
                SELECT
                    Date AS date,
                    UPPER(Vessel) AS vessel,
                    COUNT(DISTINCT Date) As "Rental of Weight",
                    GROUP_CONCAT(
                        DISTINCT "Side Working",
                        ', '
                        ORDER BY
                            "Side Working"
                    ) As "Sides Working",
                    "Rental of Weight" * 50 AS Rental,
                    COUNT(DISTINCT "Side Working") * 50 As Calibration,
                    Rental + Calibration AS Total
                FROM
                    raw_ops_dataf
                GROUP BY
                    Date,
                    Vessel
                ORDER BY
                    Date ASC
            ),joined AS (
        SELECT * FROM sto s JOIN tare t ON t.date BETWEEN s.starting_date AND s.ending_date AND t.vessel = s.vessel

            )


        SELECT id,vessel,SUM(Rental) AS rental_price,SUM(Calibration) AS calibration,SUM(Total) AS total FROM joined GROUP BY id,vessel
        """
    )
    return (tare_dataf,)


@app.cell
def _():
    from data_source.make_dataset import load_excel
    from data_source.excel_file_path import ExcelFiles
    return ExcelFiles, load_excel


@app.cell
def _(ExcelFiles, load_excel):
    handling_dataf = load_excel(file_path=ExcelFiles.OPERATIONS_ACTIVITY)
    return (handling_dataf,)


@app.cell
def _(handling_dataf, sto_dataf):
    well_dataf = mo.sql(
        f"""
        WITH
            sto AS (
                SELECT
                    *
                FROM
                    sto_dataf
            ),
            well AS (
                SELECT
                    DAY AS day_name,
                    DATE AS date,
                    "VESSEL NAME" AS vessel,
                    COALESCE("Well-to-Well Transfer", .0) AS well_to_well
                FROM
                    handling_dataf
                WHERE
                    date NOT NULL
                    AND well_to_well > 0
            ),
            joined AS (
                SELECT
                    *,
                    CASE
                        WHEN day_name IN ('Sun', 'PH') THEN 25.5
                        ELSE 17
                    END AS price
                FROM
                    sto s
                    JOIN well w ON w.date BETWEEN s.starting_date AND s.ending_date
                    AND s.vessel = w.vessel
            ),
            add_price AS (
                SELECT
                    *,
                    well_to_well * price AS total_price
                FROM
                    joined
            )
        SELECT
            id,vessel,SUM(well_to_well) AS well_to_well,SUM(total_price) AS total
        FROM
            add_price
        GROUP BY 
        	id,vessel
        ORDER BY id
        """
    )
    return (well_dataf,)


@app.cell
def _(handling_dataf, sto_dataf):
    extra_dataf = mo.sql(
        f"""
        WITH
            sto AS (
                SELECT
                    *
                FROM
                    sto_dataf
            ),
            extra AS (
                SELECT
                    DAY AS day_name,
                    DATE AS date,
                    "VESSEL NAME" AS vessel,
                    ROUND("TOTAL TONNAGE", 3) AS tonnage,
                    COALESCE("Extra Men", 0) AS extra_men,
                    --   "Number of Stevedores" AS num_stevedores,
                    -- CASE 
                    -- 	WHEN 
                    -- 		num_stevedores <47 THEN 0 ELSE 
                    -- 		num_stevedores - 47 
                    -- END AS extra_check,
                    --  extra_check = extra_men
                FROM
                    handling_dataf
                WHERE
                    date IS NOT NULL
            ),joined AS (
        SELECT
            *,
            CASE
                WHEN day_name IN ('Sun', 'PH') THEN 1.2
                ELSE 0.8
            END AS price,
            ROUND(tonnage * extra_men * price) AS total
        FROM
            sto s
            JOIN extra e ON e.date BETWEEN s.starting_date AND s.ending_date
            AND s.vessel = e.vessel

            )


        SELECT id,vessel,SUM(total) AS total FROM joined GROUP BY id,vessel
        """
    )
    return (extra_dataf,)


@app.cell
def _(ExcelFiles, load_excel):
    additional_dataf = load_excel(ExcelFiles.ADDITIONAL_OVERTIME).select(
        [
            "Day_1",
            "Date_1",
            "Vessel_1",
            "Tonnage",
            "Num of Stevedores",
            "Additional Stevedores ($)",
        ]
    )
    return (additional_dataf,)


@app.cell
def _(additional_dataf, sto_dataf):
    add_dataf = mo.sql(
        f"""
        WITH
            sto AS (
                SELECT
                    *
                FROM
                    sto_dataf
            ),
            additional AS (
                SELECT
                    Date_1 AS date,
                    UPPER(Vessel_1) AS vessel,
                    ROUND(Tonnage, 3) AS tonnage,
                    "Additional Stevedores ($)" AS total
                FROM
                    additional_dataf
                WHERE
                    "Date_1" IS NOT NULL
            ),
            joined AS (
                SELECT
                    *
                FROM
                    sto s
                    JOIN additional a ON a.date BETWEEN s.starting_date AND s.ending_date
                    AND s.vessel = a.vessel
            )
        SELECT
            id,
            vessel,
            SUM(total) AS total
        FROM
            joined
        GROUP BY
            id,
            vessel
        HAVING
            total > 0
        """
    )
    return (add_dataf,)


@app.cell
def _():
    from dataframe.transport import forklift
    return (forklift,)


@app.cell
def _(forklift, sto_dataf):
    fork_dataf = mo.sql(
        f"""
        WITH
            sto AS (
                SELECT
                    *
                FROM
                    sto_dataf
            ),
            fork AS (
                SELECT
                    *
                FROM
                    forklift
                WHERE
                    invoiced_in NOT IN ('SAPMER', 'ECHEBASTAR', 'HARTSWATER LIMITED')
                    AND YEAR(date) = 2025
            ),
            joined AS (
                SELECT
                    *
                FROM
                    sto s
                    JOIN fork f ON f.date BETWEEN s.starting_date AND s.ending_date
                    AND f.customer = s.vessel
            )
        SELECT
            id,
            vessel,
            (CEIL(SUM(overtime_150) / 60) * 45) + (CEIL(SUM(overtime_200) / 60) * 60) + (CEIL(SUM(normal_hours) / 60) * 30) AS total,
        FROM
            joined
        GROUP BY
            id,
            vessel
        ORDER BY id
        """
    )
    return (fork_dataf,)


@app.cell
def _():
    from dataframe.shore_handling import forklift_salt, salt
    return forklift_salt, salt


@app.cell
def _(salt, sto_dataf):
    salt_dataf = mo.sql(
        f"""
        WITH
            sto AS (
                SELECT
                    *
                FROM
                    sto_dataf
            ),
            salt_ops AS (
                SELECT
                    *
                FROM
                    salt
            WHERE customer NOT IN ( 'ECHEBASTAR', 'HARTSWATER LIMITED')
            ),joined AS (
        SELECT
            s.id,
            s.vessel,
            o.tonnage,
            ROUND(o.price,3) AS total

        FROM
            sto s
            JOIN salt_ops o ON o.date BETWEEN s.starting_date AND s.ending_date
            AND s.vessel = o.vessel)


        SELECT id,vessel, SUM(total) AS total FROM joined GROUP BY id,vessel
        """
    )
    return (salt_dataf,)


@app.cell
def _(forklift_salt):
    fsalt = forklift_salt()
    return (fsalt,)


@app.cell
def _(fsalt, sto_dataf):
    fork_salt_dataf = mo.sql(
        f"""
        WITH
            sto AS (
                SELECT
                    *
                FROM
                    sto_dataf
            ),
            fork AS (
                SELECT
                    *
                from
                    fsalt
            ),
            joined AS (
                SELECT
                    *
                FROM
                    sto s
                    JOIN fork f ON f.date BETWEEN s.starting_date AND s.ending_date
                    AND s.vessel = f.vessel
                WHERE
                    customer NOT IN ('ECHEBASTAR', 'HARTSWATER LIMITED')
            ),result AS (


        SELECT
            id,
            vessel,
            CEIL(
                EXTRACT (
                    HOUR
                    FROM
                        overtime_for_normal_services
                ) + EXTRACT (
                    MINUTE
                    FROM
                        overtime_for_normal_services
                ) / 60
            ) * 45 AS ot_150,
                CEIL(
                EXTRACT (
                    HOUR
                    FROM
                        overtime_for_extended_services
                ) + EXTRACT (
                    MINUTE
                    FROM
                        overtime_for_extended_services
                ) / 60
            ) * 60 AS ot_200,
                    CEIL(
                EXTRACT (
                    HOUR
                    FROM
                        normal_hour_services
                ) + EXTRACT (
                    MINUTE
                    FROM
                        normal_hour_services
                ) / 60
            ) * 30 AS normal

        FROM
            joined)


        SELECT id,vessel,SUM(normal+ot_200+ot_150) AS total FROM result GROUP BY id,vessel
        """
    )
    return (fork_salt_dataf,)


@app.cell
def _():
    from dataframe.transport import shore_crane
    return (shore_crane,)


@app.cell
def _(shore_crane, sto_dataf):
    shore_crane_dataf = mo.sql(
        f"""
        WITH
            sto AS (
                SELECT
                    *
                FROM
                    sto_dataf
            ),
            crane AS (
                SELECT
                    *
                FROM
                    shore_crane
            WHERE YEAR(date) = 2025
            ),
            joined AS (
                SELECT
                    *
                FROM
                    sto s
                    JOIN crane c ON c.date BETWEEN s.starting_date AND s.ending_date
                    AND s.vessel = c.customer
                WHERE
                    c.invoiced_to <> 'MAERSKLINE'
            )
        SELECT
            id,
            vessel,
            ROUND(SUM(total_price), 3) AS total
        FROM
            joined
        GROUP BY
            id,
            vessel
        ORDER BY
            id
        """
    )
    return (shore_crane_dataf,)


@app.cell
def _(ExcelFiles, load_excel):
    movement_out = load_excel(ExcelFiles.MOVEMENT_OUT)
    return (movement_out,)


@app.cell
def _():
    from dataframe.transport import transfer
    return (transfer,)


@app.cell
def _(movement_out, sto_dataf, transfer):
    transfer_dataf = mo.sql(
        f"""
        WITH
            sto AS (
                SELECT
                    *
                FROM
                    sto_dataf
            ),
            moves AS (
                SELECT
                    Date AS date,
                    "Container Ref. No." AS container_number,
                    Status,
                    REPLACE(UPPER("F/Vessel"), '- OSS', ' ') AS vessel
                FROM
                    movement_out
                WHERE
                    Status = 'F'
            ),
            haul AS (
                SELECT
                    *
                FROM
                    transfer
                WHERE
                    status = 'Full'
                    AND movement_type = 'Delivery'
                    AND remarks NOT IN ('MAERSKLINE', 'IOT')
            AND YEAR(date) = 2025
            ),
            hau_move AS (
                SELECT
                    *,
                    h.date,
                    m.vessel,
                    h.haulage_price
                FROM
                    moves m
                    JOIN haul h ON m.date = h.date
                    AND h.container_number = m.container_number
            ),
            joined AS (
                SELECT
                    *
                FROM
                    sto s
                    JOIN hau_move t ON t.vessel = s.vessel
                    -- AND t.date >= s.ending_date
                    AND t.date > s.starting_date
            )
        SELECT
            id,
            vessel,
            SUM(haulage_price) AS total
        FROM
            joined

        GROUP BY id,vessel
        """
    )
    return (transfer_dataf,)


@app.cell
def _(
    electricity_dataf,
    fork_dataf,
    fork_salt_dataf,
    net_dataf,
    salt_dataf,
    shore_crane_dataf,
):


    shore_crane_dataf

    fork_salt_dataf

    salt_dataf



    electricity_dataf

    net_dataf

    fork_dataf
    return


@app.cell
def _(
    add_dataf,
    electricity_dataf,
    extra_dataf,
    fork_dataf,
    fork_salt_dataf,
    net_dataf,
    salt_dataf,
    shore_crane_dataf,
    sto_dataf,
    tare_dataf,
    transfer_dataf,
    well_dataf,
):
    sto_estimates = mo.sql(
        f"""
        WITH
            sto AS (
                SELECT
                    *
                FROM
                    sto_dataf
            ),
            tare AS (
                SELECT
                    *
                FROM
                    tare_dataf
            ),
            extra AS (
                SELECT
                    *
                FROM
                    extra_dataf
            ),
            add_ot AS (
                SELECT
                    *
                FROM
                    add_dataf
            ),
            well AS (
                SELECT
                    *
                FROM
                    well_dataf
            ),
            trans AS (
                SELECT
                    *
                FROM
                    transfer_dataf
            ),
            fork AS (
                SELECT
                    *
                FROM
                    fork_dataf
            ),
            crane AS (
                SELECT
                    *
                FROM
                    shore_crane_dataf
            ),
            sel AS (
                SELECT
                    *
                FROM
                    salt_dataf
            ),
            fork_sel AS (
                SELECT
                    *
                FROM
                    fork_salt_dataf
            ),
            elec AS (
                SELECT
                    *
                FROM
                    electricity_dataf
            ),
            net AS (
                SELECT
                    *
                FROM
                    net_dataf
            )
            -- net_dataf
        SELECT
            s.id,
            s.vessel,
            s.customer,
            s.starting_date,
            s.ending_date,
            CAST(COALESCE(t.total, 0) AS FLOAT) AS tare_weight_price,
            COALESCE(e.total, 0) AS extra_men_price,
            COALESCE(w.total, 0) AS well_to_well_price,
            COALESCE(a.total, 0) AS additional_ot_price,
            COALESCE(tr.total, 0) AS transfer_price,
            COALESCE(f.total, 0) AS forklift_price,
            COALESCE(c.total, 0) AS crane_price,
            COALESCE(sl.total, 0) AS salt_price,
            COALESCE(fs.total, 0) AS forklift_salt_price,
            COALESCE(el.total, 0) AS electricity_price,
            COALESCE(n.total, 0) AS unloading_price,
            -- (
            --     tare_weight_price + extra_men_price + well_to_well_price + additional_ot_price + transfer_price + forklift_price + crane_price + salt_price + forklift_salt_price + electricity_price + unloading_price
            -- ) AS total_price
        FROM
            sto s
            LEFT JOIN tare t ON s.id = t.id
            LEFT JOIN extra e ON e.id = s.id
            LEFT JOIN add_ot a ON a.id = s.id
            LEFT JOIN well w ON w.id = s.id
            LEFT JOIN trans tr ON tr.id = s.id
            LEFT JOIN fork f ON f.id = s.id
            LEFT JOIN crane c ON c.id = s.id
            LEFT JOIN sel sl ON sl.id = s.id
            LEFT JOIN fork_sel fs ON fs.id = s.id
            LEFT JOIN elec el ON el.id = s.id
            LEFT JOIN net n ON n.id = s.id
        ORDER BY
            s.id
        """
    )
    return (sto_estimates,)


@app.cell
def _(sto_estimates):
    _df = mo.sql(
        f"""
        SELECT
            -- vessel,
            -- customer,
            -- starting_date,
            -- ending_date,
            *,
            ROUND(
                tare_weight_price + extra_men_price + well_to_well_price + additional_ot_price + transfer_price + forklift_price + crane_price + salt_price + forklift_salt_price + electricity_price + unloading_price,
                3
            ) AS total_price
        FROM
            sto_estimates
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
