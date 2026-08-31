import marimo

__generated_with = "0.24.0"
app = marimo.App(width="full")

with app.setup:
    from concurrent.futures import ThreadPoolExecutor
    from datetime import date, timedelta
    from typing import Any

    import marimo as mo
    import polars as pl
    from data import bc_items_lf
    from dataframe import forklift, hatch_to_hatch
    from datasets import (
        coa,
        forklift_salt,
        genesis_raw,
        load_salt,
        net_list,
        oss_stuffing,
        shore_crane,
        transfer,
    )
    from save import export_dataframes
    from scan_google_sheet import scan_google_sheet

    ACTIVITY_XLSX = r"C:\Users\gmounac\Dropbox\! OPERATION SUPPORTING DOCUMENTATION\2026\2026 IPHS operation activity.xlsx"
    VALIDATION_XLSX = (
        r"P:\Verification & Invoicing\Validation Report\Validation Report.xlsx"
    )

    with ThreadPoolExecutor(max_workers=10) as _pool:
        _f_tare = _pool.submit(lambda: genesis_raw().collect())
        _f_stuffing = _pool.submit(lambda: coa().collect())
        _f_salt = _pool.submit(lambda: load_salt().collect())
        _f_forklift_salt = _pool.submit(lambda: forklift_salt().collect())
        _f_forklift = _pool.submit(lambda: forklift.collect())
        _f_well_to_well = _pool.submit(lambda: hatch_to_hatch.collect())
        _f_full_oss = _pool.submit(lambda: oss_stuffing.oss().collect())
        _f_net_list = _pool.submit(lambda: net_list().collect())
        _f_shore_crane = _pool.submit(
            lambda: shore_crane.shore_crane().collect()
        )
        _f_transfer = _pool.submit(lambda: transfer.transfer().collect())
        _f_price = _pool.submit(lambda: bc_items_lf.collect())

    def _fix_time(df: pl.DataFrame) -> pl.DataFrame:
        # DuckDB Arrow filter pushdown doesn't support TIME_NS; cast to strings.
        # SQL callers can still do col::TIME for comparisons.
        time_cols = [c for c, d in zip(df.columns, df.dtypes) if d == pl.Time]
        return (
            df.with_columns(pl.col(c).cast(pl.Utf8) for c in time_cols)
            if time_cols
            else df
        )

    TARE_DATASET = _fix_time(_f_tare.result())
    STUFFING_DATASET = _fix_time(_f_stuffing.result())
    SALT_DATASET = _fix_time(_f_salt.result())
    FORKLIFT_SALT_DATASET = _fix_time(_f_forklift_salt.result())
    FORKLIFT_DATASET = _fix_time(_f_forklift.result())
    WELL_TO_WELL_DATASET = _f_well_to_well.result()
    FULL_OSS_DATASET = _fix_time(_f_full_oss.result())
    NET_LIST_DATASET = _fix_time(_f_net_list.result())
    SHORE_CRANE_DATASET = _fix_time(_f_shore_crane.result())
    TRANSFER_DATASET = _fix_time(_f_transfer.result())
    price_df = _f_price.result()


@app.function
def format_datestr_to_month_year(date_str: str) -> str:
    """Format an ISO date string to "Month 'YY"."""
    date_converted = date.fromisoformat(date_str)
    return f"{date_converted:%B} '{date_converted:%y}"


@app.function
def get_first_value(
    df: pl.DataFrame,
    column: str,
    default: Any = None,
) -> Any:
    """First value of a column, or a default when the frame is empty."""
    if df.is_empty() or column not in df.columns:
        return default
    return df.get_column(column).item(0)


@app.function
def clean_affectation(df: pl.DataFrame) -> pl.DataFrame:
    """Clean the affectation dataset"""
    return df.select(
        pl.col("Date affected").alias("date"),
        pl.col("Container Ref. No.").alias("container_number"),
        pl.col("Assigned to").str.to_uppercase().alias("vessel"),
        pl.col("Date Gate Out").alias("exit_date"),
    )


@app.cell
def _():
    mode = mo.ui.dropdown(
        label="Report Type",
        options=["OSS", "STO", "Extended OSS"],
        value="OSS",
    )
    return (mode,)


@app.cell
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    well_inv_df = scan_google_sheet(
        url="https://docs.google.com/spreadsheets/d/1PvTkl6DYZdhtaiNshz0qwtSPxC8S1OOeu905NmhFKNs/edit?gid=1301483182#gid=1301483182",
        sheet_name="WelltoWell",
    )
    return (well_inv_df,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    berth_ldf = pl.read_excel(
        source=ACTIVITY_XLSX,
        table_name="berth_dues",
        schema_overrides={"TIME IN": pl.Time, "TIME OUT": pl.Time},
    )

    additional_ldf = pl.read_excel(
        source=ACTIVITY_XLSX,
        sheet_name="Additional Stevedores",
        schema_overrides={"End Time": pl.Time},
    )

    extra_men_ldf = pl.read_excel(
        source=ACTIVITY_XLSX,
        table_name="handling_activity",
    )
    return additional_ldf, berth_ldf, extra_men_ldf


@app.cell(hide_code=True)
def _(extra_men_ldf, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    EXTRA_MEN_DATASET_NEW = mo.sql(
        """
        WITH main AS (FROM
            extra_men_ldf
        SELECT
            "DAY" AS day_name,
            "DATE" AS date,
            "VESSEL NAME" AS vessel,
            "TOTAL TONNAGE"::DECIMAL AS total_tonnage,
            "Extra Men" AS extra_men,
        CASE WHEN "DAY" IN ('Sun','PH') THEN 1.2 ELSE 0.8 END AS price,
            "Comments" AS remarks
        WHERE "Extra Men" >0)


        FROM main 
        SELECT * EXCLUDE remarks,
        price * total_tonnage * extra_men AS total_price,
        remarks
        """,
        output=False
    )


@app.cell(hide_code=True)
def _(additional_ldf, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    ADDITIONAL_OVERTIME_DATASET_NEW = mo.sql(
        """
        FROM additional_ldf
        SELECT 
        Day AS day_name,
        Date::DATE + "End Time"::TIME AS date,
        Vessel AS vessel,
        "Num of Stevedores" AS number_of_stevedores,
        "Hours" AS number_of_hours,
        "Tonnage" AS overtime_tonnage,
        8 AS unit_price,
        ("Num of Stevedores"::INT * "Hours"::INT) * 8 AS total_price,
        Remarks
        WHERE "Num of Stevedores" <> 'To check'
        """,
        output=False
    )


@app.cell
def _():
    select_report = mo.ui.dropdown(
        label="Select Line",
        options=[
            "ACILA",
            "ADAMAS",
            "AFFINIS",
            "ALAKRANA",
            "ALAKRANTXU",
            "ALBACAN",
            "ALBACORA CUATRO",
            "ALBACORA UNO",
            "ALBATUN DOS",
            "ALBATUN TRES",
            "ALPHA GOLD No. 66",
            "ALTARRI",
            "ARCHANDA",
            "ARTZA",
            "ARWA",
            "ASIAN MARINE REEFER",
            "ATERPE ALAI",
            "AURIGA",
            "AVEL VAD",
            "AVUNDA REEFER",
            "BALBAYA",
            "BELLE ISLE",
            "BELLE RIVE",
            "BELOUVE",
            "BERNICA",
            "BETI AURRERA",
            "BLUE OCEAN",
            "BOYANG CAPELLA",
            "CAP SAINT VINCENT",
            "CAP SAINTE MARIE",
            "CAPE CORAL",
            "DOLOMIEU",
            "DONIENE",
            "DRACO",
            "EGALABUR",
            "ELAI ALAI",
            "EUSKADI ALAI",
            "FRANCHE TERRE",
            "FU KUO No. 8",
            "GALERNA DOS",
            "GALERNA LAU",
            "GALERNA TRES",
            "GEVRED",
            "GLENAN",
            "GOLDEN FULL No. 168",
            "GREEN AUSTEVOLL",
            "GREEN BODO",
            "GREENSEA BERMEO",
            "GUAYATUNA UNO",
            "HAIZEA BOST",
            "HAIZEA HIRU",
            "HAIZEA LAU",
            "HAIZEA SEI",
            "HAWWA",
            "HSIANG PERNG No. 212",
            "IGS SANKALP",
            "INDO PRINCE",
            "INS IKSHAK",
            "INS KESARI",
            "INS TARKASH",
            "INS TIR",
            "INTERTUNA TRES",
            "ITSAS TXORI",
            "IZAR ARGIA",
            "HAI YANG 26",
            "IZARO",
            "IZURDIA",
            "JAI ALAI",
            "JANVIER LOUIS RAPHAEL",
            "JO WEN",
            "KERSAINT",
            "LAYLA",
            "MAN AN",
            "MERCURY",
            "NF DAFA No. 8",
            "NF EASTERN STAR",
            "NF INDIAN TUNA No. 9",
            "NF SEA GLORY No. 16",
            "NF TUNA PEAK",
            "NF TUNA PEAK No. 1",
            "NO. 121 DONGWON",
            "NOUR",
            "ORANGE SEA",
            "ORANGE SPIRIT",
            "ORANGE STREAM",
            "PACIFIC STAR",
            "PENDRUC",
            "PLAYA DE ANZORAS",
            "PLAYA DE ARITZATXU",
            "PLAYA DE AZKORRI",
            "PLAYA DE LAIDA",
            "PLAYA DE RIS",
            "SALGIR",
            "SIERRA LARA",
            "SIERRA LAUREL",
            "SIERRA LEYRE",
            "SIERRA LYRE",
            "SIN HUA FONG",
            "TALENDUIC",
            "TANG SHAN 122",
            "TORRE ITALIA",
            "TREVIGNON",
            "TXORI ARGI",
            "TXORI AUNDI",
            "TXORI BAT",
            "TXORI BERRI",
            "TXORI BI",
            "TXORI GORRI",
            "TXORI TOKI",
            "TXORI ZURI",
            "VASCO",
            "WOENFULL No. 668",
            "YI FENG",
            "YI FENG 3",
            "ZAYN",
        ],
        value="PACIFIC STAR",
        searchable=True,
    )
    return (select_report,)


@app.cell(hide_code=True)
def _(berth_ldf, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    berth_df = mo.sql(
        """
        FROM berth_ldf
        SELECT "VESSEL NAME" AS vessel,
        "DATE IN"::DATE + "TIME IN"::TIME AS date_in,
        "DATE OUT"::DATE + "TIME OUT"::TIME AS date_out,
        "DURATION IN PORT" AS duration_in_port,
        "VALUE ($)" AS "value",
        COALESCE("DISCOUNT",0) AS discount,
        "INVOICE VALUE" AS invoice_value,
        "INVOICING ENTITY" AS invoicing_entity,
        "VESSEL TYPE" AS vessel_type,
        "COMMENTS" AS remarks
        """,
        output=False
    )
    return (berth_df,)


@app.cell
def _(berth_df, select_report, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    _options = (
        berth_df.filter(pl.col("vessel").eq(select_report.value))
        .select(pl.col("date_in").dt.date().dt.strftime(format="%Y-%m-%d"))
        .unique()
        .sort("date_in")
        .to_series()
        .to_list()
    )

    start_date = mo.ui.dropdown(
        options=_options,
        value=_options[0] if _options else None,
        label="Start Date",
    )
    return (start_date,)


@app.cell
def _(berth_df, select_report, start_date, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.stop(
        start_date.value is None,
        mo.callout(
            mo.md(
                f"**No berth record found for {select_report.value}.** "
                "Select a different vessel."
            ),
            kind="warn",
        ),
    )

    end_date = (
        berth_df.filter(
            pl.col("vessel")
            .eq(select_report.value)
            .and_(
                pl.col("date_in")
                .dt.date()
                .eq(date.fromisoformat(start_date.value))
            )
        )
        .select(pl.col("date_out").dt.date().max())
        .item()
    )

    mo.stop(
        end_date is None,
        mo.callout(
            mo.md(
                f"**Missing DATE OUT** for {select_report.value} on "
                f"{start_date.value}. Complete the berth-dues sheet first."
            ),
            kind="warn",
        ),
    )
    return (end_date,)


@app.cell
def _(copy_button, customer, document_date, metrics_df, period_end, period_start, posting_date, save_button, select_report, start_date, sto_number, summary_table, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    title = mo.md(f"# ⛴️ {mode.value} Report")

    filter_bar = mo.hstack(
        [
            select_report,
            start_date,
        ],
        justify="start",
        gap=2,
    )

    meta = mo.hstack(
        [
            mo.stat(
                label="Invoice Value",
                value=f"{metrics_df.item():,.2f}",
                caption="Total price (USD)",
            ),
        ],
        justify="start",
        gap=1,
    )

    actions = mo.hstack([copy_button, save_button], justify="start", gap=1)

    mo.vstack(
        [
            title,
            mo.md("---"),
            filter_bar,
            customer,
            meta,
            mo.md("---"),
            document_date,
            posting_date,
            period_start,
            period_end,
            sto_number,
            summary_table,
            mo.md("---"),
            actions,
        ]
    )


@app.cell
def _(bc_df, copy_button, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.stop(not copy_button.value)
    bc_df.write_clipboard()  # tab-separated, pastes cleanly into BC / Excel
    mo.md("✅ Copied to clipboard")


@app.cell
def _():
    report_status_df = scan_google_sheet(
        url="https://docs.google.com/spreadsheets/d/1xNh4SiP_xw8Ck1baLhFazBsmMLIbOHjF4tg_M89efvI/edit?gid=1899664910#gid=1899664910",
        sheet_name="report_status",
    )
    return (report_status_df,)


@app.cell(hide_code=True)
def _(berth_ldf, report_status_df, select_report, start_date, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    report_data_df = mo.sql(
        f"""
        WITH
            main AS (
                FROM
                    berth_ldf
                SELECT
                    "VESSEL NAME" AS vessel,
                    "DATE IN"::DATE + "TIME IN"::TIME AS start_date,
                    "DATE OUT"::DATE + "TIME OUT"::TIME AS end_date,
                WHERE
                    "VESSEL NAME" = '{select_report.value}'
                    AND "DATE IN" = '{start_date.value}'
            ),
            report AS (
                FROM
                    report_status_df
                SELECT
                    "month" AS report_month,
                    sub_type AS sto_number,
                    "vessel/client" AS vessel,
                    customer,
                    start_date,
                    end_date
                WHERE
                    report_type = '{"OSS" if mode.value == "Extended OSS" else "STO"}'
                    AND "vessel/client" = '{select_report.value}'
                    AND start_date = '{start_date.value}'
            )
        SELECT 
            r.report_month,


            r.customer,
            r.end_date AS document_date,
            r.end_date AS posting_date,
            m.start_date AS period_start,
            m.end_date AS period_end,
            r.sto_number,
            r.vessel

        FROM
            report r
            LEFT JOIN main m ON m.vessel = r.vessel
            AND m.start_date::DATE = r.start_date
            AND m.end_date::DATE = r.end_date
        """,
        output=False
    )
    return (report_data_df,)


@app.cell
def _(report_data_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    document_date = mo.ui.text(
        label="Document Date:",
        value=str(
            get_first_value(report_data_df, "document_date", default="")
        ),
    )

    posting_date = mo.ui.text(
        label="Posting Date:",
        value=get_first_value(
            report_data_df,
            "posting_date",
            default="",
        ),
    )

    period_start = mo.ui.text(
        label="Period Start:",
        value=str(get_first_value(report_data_df, "period_start", default="")),
    )

    period_end = mo.ui.text(
        label="Period End:",
        value=str(get_first_value(report_data_df, "period_end", default="")),
    )

    sto_number = mo.ui.text(
        label="StopOver No.",
        value=str(get_first_value(report_data_df, "sto_number", default="")),
    )

    customer = mo.ui.text(
        label="Customer Name",
        value=str(get_first_value(report_data_df, "customer", default="")),
    )
    return (
        customer,
        document_date,
        period_end,
        period_start,
        posting_date,
        sto_number,
    )


@app.cell
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    copy_button = mo.ui.run_button(label="📋 Copy BC data to clipboard")
    save_button = mo.ui.run_button(label="💾 Save XLSX report")
    return copy_button, save_button


@app.cell(hide_code=True)
def _(forklift_hours, no_of_plugin, number_of_containers, salt_operations, total_tonnage, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    summary_table = mo.Html(
        f"""
        <table style="
            margin: 0 auto;
            border-collapse: collapse;
            font-size: 16px;
            line-height: 1.45;
        ">
            <tr>
                <td style="text-align:right; padding-right:28px; font-style:italic;">
                    Plugin
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {no_of_plugin}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Plugins
                </td>
            </tr>
            <tr>
                <td style="text-align:right; padding-right:28px; font-style:italic;">
                    Tonnage
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {total_tonnage}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Tons
                </td>
            </tr>
             <tr>
                <td style="text-align:right; padding-right:28px; font-style:italic;">
                    Total Container Stuffed
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {number_of_containers}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Containers
                </td>
            </tr>
            <tr>
                <td style="text-align:right; padding-right:28px; font-style:italic;">
                    Forklift Services:
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {forklift_hours}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Hours
                </td>
                </tr>
            <tr>
                <td style="text-align:right; padding-right:28px; font-style:italic;">
                    Salt Operations:
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {salt_operations}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Hours
                </td>
            </tr>
            <tr>
                <td style="text-align:right; padding-right:28px; font-style:italic;">
                    Forklift Services:
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {forklift_hours}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Hours
                </td>
            </tr>
            <tr>
                <td colspan="3" style="height:18px;"></td>
            </tr>
        </table>
        """
    )
    return (summary_table,)


@app.cell
def _(final_berth_df, select_report, start_date, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.callout(
        mo.md(
            f"**No berth record found** for **{select_report.value}** "
            f"starting **{start_date.value}**. "
            "Check the vessel name or select a different start date."
        ),
        kind="warn",
    ) if final_berth_df.is_empty() else None


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    ## Datasets
    """)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    ### Berth Dues :⚓
    """)


@app.cell(hide_code=True)
def _(berth_df, select_report, start_date, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    final_berth_df = mo.sql(
        f"""
        FROM
            berth_df 
        WHERE
            vessel = '{select_report.value}'
            AND date_in::DATE = '{start_date.value}'
        """
    )
    return (final_berth_df,)


@app.cell
def _(final_berth_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    discount_berthing_figure: int = final_berth_df.select(
        pl.col("discount").sum()
    ).item()
    value_berthing_figure: int = final_berth_df.select(
        pl.col("value").sum()
    ).item()
    return discount_berthing_figure, value_berthing_figure


@app.cell
def _(discount_berthing_figure: int, value_berthing_figure: int, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.stop(mode.value != "STO")
    berth_discount = mo.stat(value=discount_berthing_figure,label="Discount ($)",bordered=True)
    berth_value = mo.stat(value=value_berthing_figure,label="Berthing ($)",bordered=True)
    berth_total_value = mo.stat(value=(value_berthing_figure + discount_berthing_figure),label="Total Berthing ($)",bordered=True)

    mo.vstack([mo.md("## Berthing"),berth_value,berth_discount,berth_total_value],justify="center")


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    ### Tare Weight ::lucide:weight::
    """)


@app.cell(hide_code=True)
def _(end_date, select_report, start_date, tare_dataset, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    tare_df = mo.sql(
        f"""
        WITH
            main AS (
                FROM
                    TARE_DATASET
                SELECT
                    date AS date,
                    UPPER(vessel) AS vessel,
                    1 AS rental,
                    COUNT(DISTINCT "Side Working") AS number_of_side_working,
                    STRING_AGG(DISTINCT "Side Working", ',') AS side_working,
                    (50) AS rental_price,
                    (50) AS calibration_price,
                GROUP BY ALL
                HAVING
                    vessel = '{select_report.value}'
                    AND date BETWEEN '{start_date.value}' AND '{end_date}'
                ORDER BY
                    date
            )
        FROM
            main
        SELECT
            date,
            rental AS rental_of_weight,
            side_working,
            rental_price,
            (calibration_price * number_of_side_working) AS calibration_price,
        rental_price +  (calibration_price * number_of_side_working) AS total_price
        """
    )
    return (tare_df,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    ### Calibration Summary
    """)


@app.cell(hide_code=True)
def _(end_date, select_report, start_date, tare_dataset, unpivoted, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    tare_summary_df = mo.sql(
        f"""
        WITH
            main AS (
                FROM
                    TARE_DATASET
                SELECT
                    date AS date,
                    UPPER(vessel) AS vessel,
                    1 AS rental,
                    COUNT(DISTINCT "Side Working") AS number_of_side_working,
                    STRING_AGG(DISTINCT "Side Working", ',') AS side_working,
                    (50) AS rental_price,
                    (50) AS calibration_price,
                GROUP BY ALL
                HAVING
                    vessel = '{select_report.value}'
                    AND date BETWEEN '{start_date.value}' AND '{end_date}'
                ORDER BY
                    date
            ),


        summary AS (
            SELECT
                SUM(rental) AS rental,
                SUM(number_of_side_working) AS calibration
            FROM main
        ),

        unpivoted AS (
            UNPIVOT summary
            ON rental, calibration
            INTO
                NAME service
                VALUE quantity
        )

        SELECT
            service,
            quantity,
            -- 50 AS unit_price,
            quantity * 50 AS total_price
        FROM unpivoted;
        """
    )
    return (tare_summary_df,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    ### Stuffing ::lucide:container::
    ----
    """)


@app.cell(hide_code=True)
def _(end_date, select_report, start_date, stuffing_dataset, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    stuffing_df = mo.sql(
        f"""
        FROM
            STUFFING_DATASET
        SELECT
            vessel_client AS vessel,
            date_plugged::DATE + time_plugged::TIME AS datetime_plugged_in,
            container_number,shipping_line,
            plugged_status,
            tonnage,
            set_point,
            date_out,
            CASE
                WHEN {"operation_type LIKE '%OSS%' OR " if mode.value == "STO" else ""}shipping_line = 'IOT' THEN 0
                ELSE days_on_plug
            END AS days_on_plug,
            CASE
                WHEN {"operation_type LIKE '%OSS%' OR " if mode.value == "STO" else ""}shipping_line = 'IOT' THEN 0
                ELSE total
            END AS total_price,
            CASE
                {"WHEN customer = 'MAERSKLINE' THEN 'On the account of Maersk'" if mode.value == "STO" else ""}
                WHEN customer = 'IOT' THEN 'On the account of IOT'
                ELSE ''
            END AS remarks
        WHERE
            vessel_client = '{select_report.value}'
            AND date_plugged BETWEEN '{start_date.value}' AND '{end_date}'
        ORDER BY datetime_plugged_in
        """
    )
    return (stuffing_df,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    * Stuffing Metrics for summary
    """)


@app.cell
def _(stuffing_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    # STO summary figures shown in the UI and on the Summary sheet.
    no_of_plugin = stuffing_df.filter(pl.col("remarks").is_null()).height
    days_on_electricity = (
        stuffing_df.filter(pl.col("remarks").is_null())
        .get_column("days_on_plug")
        .sum()
        or 0
    )
    number_of_containers = stuffing_df.select(
        pl.col("container_number").unique_counts().sum()
    ).item()
    return days_on_electricity, no_of_plugin, number_of_containers


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    ### Salt
    """)


@app.cell(hide_code=True)
def _(end_date, salt_dataset, select_report, start_date, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    salt_df = mo.sql(
        f"""
        FROM SALT_DATASET
        WHERE customer NOT IN ('HARTSWATER LIMITED','ECHEBASTAR') AND vessel = '{select_report.value}'  AND date BETWEEN '{start_date.value}' AND '{end_date}'
        """
    )
    return (salt_df,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    ### Summary of Salt Ops
    """)


@app.cell(hide_code=True)
def _(salt_df, unpivoted, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    salt_summary_df = mo.sql(
        """
        WITH
            summaries AS (
                FROM
                    salt_df
                SELECT
                    SUM(normal_tonnage)::DECIMAL AS "Normal Hours",
                    SUM(overtime_150_tonnage)::DECIMAL AS "Overtime 150%",
                    SUM(overtime_200_tonnage)::DECIMAL AS "Overtime 200%"
            ),
            unpivoted AS (
                UNPIVOT summaries ON "Normal Hours",
                "Overtime 150%",
                "Overtime 200%" INTO NAME service VALUE quantity
            )
        FROM
            unpivoted
        SELECT
            *,
            CASE
                WHEN service = 'Normal Hours' THEN 10 * 1.0 * quantity
                WHEN service = 'Overtime 150%' THEN 10 * 1.5 * quantity
                WHEN service = 'Overtime 200%' THEN 10 * 2.0 * quantity
            END AS total_price
        """
    )
    return (salt_summary_df,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    * Salt metrics for summary
    """)


@app.cell
def _(salt_summary_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    salt_operations = salt_summary_df.select(pl.col("quantity").sum()).item()
    return (salt_operations,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    ### Forklift ::lucide:forklift::
    """)


@app.cell
def _(end_date, select_report, start_date, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    pl.read_excel(
        source=r"C:\Users\gmounac\Dropbox\Container and Transport\Transport Section\Forklift Usage\Forklift Record.xlsx",
        schema_overrides={
            "Time Out": pl.Time,
            "Time In": pl.Time,
            "Duration": pl.Time,
        },
    ).filter(
        pl.col("Invoiced in:").is_in(["ECHEBASTAR", "HARTSWATER"]).not_()
    ).filter(
        pl.col("Vessel/Client")
        .eq(select_report.value)
        .and_(
            pl.col("Date of Service").is_between(
                date.fromisoformat(start_date.value),
                end_date + timedelta(days=5),
            )
        )
    ).sort(["Date of Service", "Time Out"])


@app.cell(hide_code=True)
def _(end_date, forklift_salt_dataset, select_report, start_date, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    f_salt_df = mo.sql(
        f"""
        FROM FORKLIFT_SALT_DATASET
        WHERE customer NOT IN ('HARTSWATER LIMITED','ECHEBASTAR') AND vessel = '{select_report.value}'  AND date BETWEEN '{start_date.value}' AND '{end_date}'
        """
    )
    return (f_salt_df,)


@app.cell(hide_code=True)
def _(end_date, forklift_dataset, select_report, start_date, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    forklift_df = mo.sql(
        f"""
        FROM FORKLIFT_DATASET
        WHERE invoiced_in NOT IN ('HARTSWATER LIMITED','ECHEBASTAR','SAPMER') AND customer = '{select_report.value}'  AND date BETWEEN '{start_date.value}' AND '{end_date}'::DATE+ INTERVAL 5 DAY
        """
    )
    return (forklift_df,)


@app.cell(hide_code=True)
def _(forklift_df, unpivoted, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    forklift_summary_df = mo.sql(
        """
        WITH
            summaries AS (FROM forklift_df
        SELECT 
        CEIL(SUM(normal_hours) / 60 ) AS "Normal Hours",
        CEIL(SUM(overtime_150) / 60 ) AS "Overtime 150%",
        CEIL(SUM(overtime_200) / 60 ) AS "Overtime 200%"),

         unpivoted AS (
                UNPIVOT summaries ON "Normal Hours",
                "Overtime 150%",
                "Overtime 200%" INTO NAME service VALUE quantity
            )



        FROM
            unpivoted
        SELECT
            *,
            CASE
                WHEN service = 'Normal Hours' THEN 30 * 1.0 * quantity
                WHEN service = 'Overtime 150%' THEN 30* 1.5 * quantity
                WHEN service = 'Overtime 200%' THEN 30* 2.0 * quantity
            END AS total_price
        """
    )
    return (forklift_summary_df,)


@app.cell(hide_code=True)
def _(f_salt_df, unpivoted, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    forklift_salt_summary_df = mo.sql(
        """
        WITH
            summaries AS (
                SELECT
                    CEIL(SUM(EPOCH(normal_hour_services::TIME)) / 3600) AS "Normal Hours",
                    CEIL(
                        SUM(EPOCH(overtime_for_normal_services::TIME)) / 3600
                    ) AS "Overtime 150%",
                    CEIL(
                        SUM(EPOCH(overtime_for_extended_services::TIME)) / 3600
                    ) AS "Overtime 200%"
                FROM
                    f_salt_df
            ),
            unpivoted AS (
                UNPIVOT summaries ON "Normal Hours",
                "Overtime 150%",
                "Overtime 200%" INTO NAME service VALUE quantity
            )
        FROM
            unpivoted
        SELECT
            *,
            CASE
                WHEN service = 'Normal Hours' THEN 30 * 1.0 * quantity
                WHEN service = 'Overtime 150%' THEN 30 * 1.5 * quantity
                WHEN service = 'Overtime 200%' THEN 30 * 2.0 * quantity
            END AS total_price
        """
    )
    return (forklift_salt_summary_df,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    * Forklift metrics for summary
    """)


@app.cell
def _(forklift_salt_summary_df, forklift_summary_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    forklift_hours = int(
        pl.concat([forklift_summary_df, forklift_salt_summary_df])
        .select(pl.col("quantity").sum())
        .item()
    )
    return (forklift_hours,)


@app.cell(hide_code=True)
def _(f_salt_df, forklift_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    final_forklift_df = mo.sql(
        """
        WITH main AS (FROM
            forklift_df
        SELECT
            day AS day_name,
            date,
            customer AS vessel,
            start_time,
            end_time,
            duration AS hours_of_service,
            service_type AS purpose),salt AS (
            FROM f_salt_df
        SELECT day AS day_name,
        date,
        vessel,
        start_time,
        end_time,
        total_duration AS hours_of_service,
        purpose
            )


        FROM main 
        UNION ALL
        FROM salt
        ORDER BY date,start_time
        """
    )
    return (final_forklift_df,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    ### Handling Activity
    ---

    #### Extra Men
    """)


@app.cell(hide_code=True)
def _(end_date, extra_men_dataset_new, select_report, start_date, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    extra_men_df = mo.sql(
        f"""
        FROM
            EXTRA_MEN_DATASET_NEW
        WHERE
            vessel = '{select_report.value}'
            AND date BETWEEN '{start_date.value}' AND '{end_date}'
        """
    )
    return (extra_men_df,)


@app.cell(hide_code=True)
def _(extra_men_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    summary_extra_men_df = mo.sql(
        """
        WITH
            normal AS (
                FROM
                    extra_men_df
                SELECT
                    'Normal Hour' AS description,
                    COALESCE(SUM(total_tonnage * extra_men::INT)::DECIMAL,0) AS value,
                    COALESCE(MIN(price),0.8::DECIMAL) AS unit_price
                WHERE
                    price = 0.8
            ),
            overtime AS (
                FROM
                    extra_men_df
                SELECT
                    'Overtime Hour' AS description,
                    COALESCE(SUM(total_tonnage * extra_men::INT)::DECIMAL,0) AS value,
                    COALESCE(MIN(price),1.2::DECIMAL) AS unit_price
                WHERE
                    price = 1.2
            ),
            grouped AS (
                FROM
                    normal
                UNION ALL
                FROM
                    overtime
            )
        FROM
            grouped
        SELECT
            *,
            (value * unit_price)::DECIMAL AS total_price
        """
    )
    return (summary_extra_men_df,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    #### Additional Overtime
    """)


@app.cell(hide_code=True)
def _(additional_overtime_dataset_new, end_date, select_report, start_date, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    additional_overtime_df = mo.sql(
        f"""
        FROM ADDITIONAL_OVERTIME_DATASET_NEW
        WHERE
            vessel = '{select_report.value}'
            AND date::DATE BETWEEN '{start_date.value}' AND '{end_date}'
        ORDER BY date
        """
    )
    return (additional_overtime_df,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    #### Well to Well
    """)


@app.cell(hide_code=True)
def _(end_date, select_report, start_date, well_inv_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    well_to_well_df = mo.sql(
        f"""
        WITH main AS (FROM well_inv_df
            SELECT "Day" AS day_name,
            "Date" AS date,
            "Vessel" AS vessel,
            "Tonnage" AS tonnage,
            CASE WHEN "Day" IN ('Sun','PH') THEN 17*1.5 ELSE 17 END AS unit_price
        WHERE
            vessel = '{select_report.value}'
            AND date BETWEEN '{start_date.value}' AND '{end_date}')

        FROM main
        SELECT *, tonnage * unit_price AS total_price
        """
    )
    return (well_to_well_df,)


@app.cell(hide_code=True)
def _(well_to_well_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    summary_well_to_well_df = mo.sql(
        """
        WITH main AS (FROM
            well_to_well_df
        SELECT
            CASE
                WHEN day_name IN ('Sun', 'PH') THEN 'Overtime'
                ELSE 'Normal'
            END AS overtime,
            SUM(tonnage) AS total_tonnage
        GROUP BY ALL),sort AS (SELECT *
            FROM (
                VALUES
                    ('Normal', 1),
                    ('Overtime', 2),
            ) AS t(overtime, sort))

        SELECT s.overtime,COALESCE(m.total_tonnage,0) AS total_tonnage
        FROM sort s LEFT JOIN main m ON m.overtime = s.overtime
        ORDER BY s.sort
        """
    )
    return (summary_well_to_well_df,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    ### Net List
    """)


@app.cell(hide_code=True)
def _(end_date, net_list_dataset, select_report, start_date, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    _full_oss_override = """CASE
                    WHEN service = 'Full OSS' AND overtime = 'normal hours'
                        THEN 38.75::DECIMAL
                    WHEN service = 'Full OSS' AND overtime = 'overtime 150%'
                        THEN 58.125::DECIMAL
                    WHEN service = 'Full OSS' AND overtime = 'overtime 200%'
                        THEN 77.5::DECIMAL
                    ELSE unit_price
                END"""
    net_list_df = mo.sql(
        f"""
        WITH adjusted AS (
            SELECT
                * EXCLUDE(unit_price, remarks),
                {_full_oss_override if mode.value == "Extended OSS" else "unit_price"} AS unit_price,
                remarks
            FROM NET_LIST_DATASET
            WHERE
                vessel = '{select_report.value}'
                AND date BETWEEN '{start_date.value}' AND '{end_date}'
        )
        SELECT
            * EXCLUDE(invoice_value),
            total_tonnage::DECIMAL * unit_price::DECIMAL AS invoice_value
        FROM adjusted
        """
    )
    return (net_list_df,)


@app.cell(hide_code=True)
def _(net_list_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    summary_net_list_df = mo.sql(
        """
        FROM net_list_df
        SELECT service,storage_type,overtime,SUM(total_tonnage)::DECIMAL AS total_tonnage,SUM(invoice_value)::DECIMAL AS total_price
        GROUP BY ALL
        ORDER BY service,overtime
        """
    )
    return (summary_net_list_df,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    * Net list metrics for summary
    """)


@app.cell
def _(summary_net_list_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    total_tonnage = summary_net_list_df.select(
        pl.col("total_tonnage").sum()
    ).item()
    return (total_tonnage,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    ### Shore Crane ::lucide:git-pull-request-create::
    """)


@app.cell(hide_code=True)
def _(end_date, select_report, shore_crane_dataset, start_date, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    shore_crane_df = mo.sql(
        f"""
        FROM SHORE_CRANE_DATASET
        WHERE
            customer = '{select_report.value}' AND invoiced_to {"=" if mode.value == "Extended OSS" else "<>"} 'MAERSKLINE'
            AND date BETWEEN '{start_date.value}' AND '{end_date}'
        """
    )
    return (shore_crane_df,)


@app.cell(hide_code=True)
def _(shore_crane_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    summary_shore_crane_df = mo.sql(
        """
        WITH
            ot_2 AS (
                FROM
                    shore_crane_df
                SELECT
                    'Overtime 200%' AS overtime,
                    SUM(overtime_hours) AS value
                WHERE
                    day_name IN ('Sun', 'PH')
            ),
            ot_1_sun AS (
                FROM
                    shore_crane_df
                SELECT
                    'Overtime 150%' AS overtime,
                    SUM(hours - overtime_hours) AS value
                WHERE
                    day_name IN ('Sun', 'PH')
            ),
            ot_1 AS (
                FROM
                    shore_crane_df
                SELECT
                    'Overtime 150%' AS overtime,
                    SUM(overtime_hours) AS value
                WHERE
                    day_name NOT IN ('Sun', 'PH')
            ),
            normal AS (
                FROM
                    shore_crane_df
                SELECT
                    'Normal Hours' AS overtime,
                    SUM(hours - overtime_hours) AS value
                WHERE
                    day_name NOT IN ('Sun', 'PH')
            ),
            -- Append the data
            grouped AS (
                FROM
                    normal
                UNION ALL
                FROM
                    ot_1
                UNION ALL
                FROM
                    ot_1_sun
                UNION ALL
                FROM
                    ot_2
            ),
            main AS (
                FROM
                    grouped
                SELECT
                    overtime,
                    SUM(value) AS value
                GROUP BY ALL
            ),
            sort_order AS (
                FROM
                    main
                SELECT
                    overtime,
                    CASE
                        WHEN overtime = 'Normal Hours' THEN 1
                        WHEN overtime = 'Overtime 150%' THEN 2
                        WHEN overtime = 'Overtime 200%' THEN 3
                        ELSE 0
                    END AS sort
            ),
            unit_price AS (
                FROM
                    main
                SELECT
                    overtime,
                    CASE
                        WHEN overtime = 'Normal Hours' THEN 135 * 1.0
                        WHEN overtime = 'Overtime 150%' THEN 135 * 1.6
                        WHEN overtime = 'Overtime 200%' THEN 135 * 2.1
                        ELSE 0
                    END AS unit_price
            )
        SELECT
            m.*,
            p.unit_price * m.value AS total_price
        FROM
            main m
            LEFT JOIN sort_order s ON s.overtime = m.overtime
            LEFT JOIN unit_price p ON p.overtime = m.overtime
        ORDER BY
            s.sort
        """
    )
    return (summary_shore_crane_df,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    ### Transfer
    """)


@app.cell
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    affectation_df = pl.read_excel(
        source=VALIDATION_XLSX,
        table_name="Affectation",
    ).pipe(clean_affectation)
    return (affectation_df,)


@app.cell(hide_code=True)
def _(affectation_df, end_date, select_report, start_date, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    to_filter_transfer_df = mo.sql(
        f"""
        FROM affectation_df
        WHERE vessel = '{select_report.value}' AND date BETWEEN '{start_date.value}' AND '{end_date}'
        """,
        output=False
    )
    return (to_filter_transfer_df,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    #### Container still on plug
    """)


@app.cell
def _(to_filter_transfer_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    container_on_plug = to_filter_transfer_df.filter(
        pl.col("exit_date").is_null()
    )
    container_on_plug


@app.cell(hide_code=True)
def _(to_filter_transfer_df, transfer_dataset, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    transfer_df = mo.sql(
        """
        WITH
            t AS (
                FROM
                    TRANSFER_DATASET
                WHERE
                    status = 'Full'
            )
        SELECT
            t.day_name,
            t.container_number,
            t.date,
            t.movement_type,
            t.destination,
            t.status,
            t.time_out::TIME AS time_out,
            CASE
                WHEN driver LIKE '%IPHS%' THEN 'IPHS'
                ELSE driver
            END AS driver,
            t."type",
            t.size,
            f.vessel,
            t.haulage_price AS price_per_move,
            t.remarks
        FROM
            t
            LEFT JOIN to_filter_transfer_df f ON f.exit_date = t.date
            AND f.container_number = t.container_number
        WHERE
            f.exit_date IS NOT NULL
           AND t.remarks NOT IN ({"'IOT', 'MAERSKLINE'" if mode.value == "STO" else "'IOT'"})
        """
    )
    return (transfer_df,)


@app.cell(hide_code=True)
def _(transfer_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    summary_transfer_df = mo.sql(
        """
        WITH
            main AS (
                FROM
                    transfer_df
                SELECT
                    COUNT(*) AS total_service,
                    CASE
                        WHEN day_name IN ('Sun', 'PH')
                        AND time_out::TIME > '16:00'::TIME THEN 'Overtime 200%'
                        WHEN (
                            day_name IN ('Sun', 'PH')
                            OR time_out::TIME > '17:00'::TIME
                        )
                        OR (
                            time_out::TIME BETWEEN '00:00'::TIME AND '08:00'::TIME
                        ) THEN 'Overtime 150%'
                        ELSE 'Normal Hours'
                    END AS overtime
                GROUP BY ALL
            ),
            sort_order AS (
                SELECT
                    *
                FROM
                    (
                        VALUES
                            ('Normal Hours', 1),
                            ('Overtime 150%', 2),
                            ('Overtime 200%', 3)
                    ) AS t (overtime, sort)
            ),
            unit_price AS (
                SELECT
                    *
                FROM
                    (
                        VALUES
                            ('Normal Hours', 90),
                            ('Overtime 150%', 90 * 1.5),
                            ('Overtime 200%', 90 * 2.0)
                    ) AS t (overtime, unit_price)
            )
        SELECT
            m.overtime,
            COALESCE(o.total_service, 0) AS total_service,
            p.unit_price * COALESCE(o.total_service, 0) AS total_price
        FROM
            sort_order m
            FULL OUTER JOIN main o ON o.overtime = m.overtime
            LEFT JOIN unit_price p ON p.overtime = m.overtime
        ORDER BY
            m.sort
        """
    )
    return (summary_transfer_df,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    ## Summaries Df (Business Central line items)
    """)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    --- Berth Dues
    """)


@app.cell(hide_code=True)
def _(final_berth_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    berth_bc = mo.sql(
        """
        WITH type_map AS (
                        SELECT * FROM (
                            VALUES
                                ('FISHING VESSEL', 'BERTH DUES (FISHING VESSELS)'),
                                ('CARGO REEFER', 'BERTH DUES (CARGO)'),
                                ('LONGLINER', 'BERTH DUES (LONGLINER)'),
                                ('SUPPLY BOAT', 'BERTH DUES (SUPPLY VESSELS)'),
                                ('MILITARY VESSEL', 'BERTH DUES (MILITARY)')
                        ) AS t (vessel_type, Description)
                    ),
                    -- one charge line per vessel type
                    charges AS (
                        SELECT
                            COALESCE(SUM(f.duration_in_port), 0) AS QTY,
                            t.Description,
                            'STD' AS Variant
                        FROM final_berth_df f
                        JOIN type_map t ON t.vessel_type = f.vessel_type
                        GROUP BY t.Description
                    ),
                    -- one discount line (QTY = -1) per vessel type with a discount
                    discounts AS (
                        SELECT
                            -1 AS QTY,
                            t.Description,
                            'STD' AS Variant
                        FROM final_berth_df f
                        JOIN type_map t ON t.vessel_type = f.vessel_type
                        WHERE f.discount < 0
                        GROUP BY t.Description
                    )
                FROM charges
                UNION ALL
                FROM discounts
        """
    )
    return (berth_bc,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    -- Tare Weight
    """)


@app.cell(hide_code=True)
def _(tare_summary_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    tare_bc = mo.sql(
        """
        WITH map AS (
                    SELECT * FROM (
                        VALUES
                            ('rental', 'TARE- RENTAL'),
                            ('calibration', 'CALIBRATION')
                    ) AS t (service, Description)
                )
                SELECT
                    s.quantity AS QTY,
                    m.Description,
                    'STD' AS Variant
                FROM tare_summary_df s
                JOIN map m ON m.service = s.service
        """,
        output=False
    )
    return (tare_bc,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    -- Stuffing
    """)


@app.cell(hide_code=True)
def _(stuffing_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    stuffing_bc = mo.sql(
        """
        WITH
            plugin AS (
                FROM
                    stuffing_df
                SELECT
                    COUNT(*) AS QTY,
                    'PLUGIN' AS Description,
                    'STD' AS Variant
                WHERE
                    remarks= ''
            ),
            monitoring AS (
                FROM
                    stuffing_df
                SELECT DISTINCT
                    COUNT(*) AS QTY,
                    'MONITORING' AS Description,
                    'STD' AS Variant
                WHERE
                    remarks= ''
                    AND plugged_status <> 'Partial'
            ),
            standard AS (
                FROM
                    stuffing_df
                SELECT
                    COALESCE(SUM(days_on_plug), 0) AS QTY,
                    'ELECTRICITY - 25°' AS Description,
                    'STD' AS Variant
                WHERE
                    set_point = -25
                    AND remarks = ''
            ),
            magnum AS (
                FROM
                    stuffing_df
                SELECT
                    COALESCE(SUM(days_on_plug), 0) AS QTY,
                    'ELECTRICITY - 35°' AS Description,
                    'STD' AS Variant
                WHERE
                    set_point = -35
                    AND remarks = ''
            ),
            s_freezer AS (
                FROM
                    stuffing_df
                SELECT
                    COALESCE(SUM(days_on_plug), 0) AS QTY,
                    'ELECTRICITY - 60°' AS Description,
                    'STD' AS Variant
                WHERE
                    set_point = -60
                    AND remarks= ''
            )
        FROM
            plugin
        UNION ALL
        FROM
            monitoring
        UNION ALL
        FROM
            standard
        UNION ALL
        FROM magnum
        UNION ALL
        FROM s_freezer
        """,
        output=False
    )
    return (stuffing_bc,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    -- Salt Operation
    """)


@app.cell(hide_code=True)
def _(salt_summary_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    salt_bc = mo.sql(
        """
        WITH variant_map AS (
                    SELECT * FROM (
                        VALUES
                            ('Normal Hours', 'STD'),
                            ('Overtime 150%', '150%'),
                            ('Overtime 200%', '200%')
                    ) AS t (service, Variant)
                )
                SELECT
                    s.quantity AS QTY,
                    'SALT LOADING' AS Description,
                    v.Variant
                FROM salt_summary_df s
                JOIN variant_map v ON v.service = s.service
        """,
        output=False
    )
    return (salt_bc,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    --- Forklift
    """)


@app.cell(hide_code=True)
def _(forklift_salt_summary_df, forklift_summary_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    forklift_bc = mo.sql(
        """
        WITH
            all_data AS (
                FROM
                    forklift_salt_summary_df
                UNION ALL
                FROM
                    forklift_summary_df
            ),
            grouped AS (
                FROM
                    all_data
                SELECT
                    service,
                    SUM(quantity) AS quantity
                GROUP BY ALL
            ),
        --- 
         normal AS (
                FROM
                    grouped
                SELECT
                    quantity AS QTY,
                    'FORKLIFT RENTAL' AS Description,
                    'STD' AS Variant
                WHERE
                    service = 'Normal Hours'
            ),
            overtime_150 AS (
                FROM
                    grouped
                SELECT
                    quantity AS QTY,
                    'FORKLIFT RENTAL' AS Description,
                    '150%' AS Variant
                WHERE
                    service = 'Overtime 150%'
            ),
            overtime_200 AS (
                FROM
                    grouped
                SELECT
                    quantity AS QTY,
                    'FORKLIFT RENTAL' AS Description,
                    '200%' AS Variant
                WHERE
                    service = 'Overtime 200%'
            )
        FROM
            normal
        UNION ALL
        FROM
            overtime_150
        UNION ALL
        FROM
            overtime_200
        """,
        output=False
    )
    return (forklift_bc,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    -- Extra Men
    """)


@app.cell(hide_code=True)
def _(summary_extra_men_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    extra_men_bc = mo.sql(
        """
        WITH normal AS (FROM summary_extra_men_df
            SELECT COALESCE(SUM(value),0) AS QTY,
            'EXTRA MEN' AS Description,
            'STD' AS Variant
        WHERE description = 'Normal Hour'),overtime AS (FROM summary_extra_men_df
            SELECT COALESCE(SUM(value),0) AS QTY,
            'EXTRA MEN' AS Description,
            '150%' AS Variant
        WHERE description = 'Overtime Hour')


        FROM normal 
        UNION ALL
        FROM overtime
        """,
        output=False
    )
    return (extra_men_bc,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    -- Additional Overtime
    """)


@app.cell(hide_code=True)
def _(additional_overtime_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    additional_overtime_bc = mo.sql(
        """
        FROM additional_overtime_df
        SELECT COALESCE(SUM(number_of_stevedores::INT * number_of_hours),0) AS QTY,
        'ADDITIONAL OVERTIME' AS Description,
        'STD' AS Variant
        """,
        output=False
    )
    return (additional_overtime_bc,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    --- Well to Well
    """)


@app.cell(hide_code=True)
def _(summary_well_to_well_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    well_to_well_bc = mo.sql(
        """
        WITH variant_map AS (
                    SELECT * FROM (
                        VALUES
                            ('Normal', 'STD'),
                            ('Overtime', '150%')
                    ) AS t (overtime, Variant)
                )
                SELECT
                    s.total_tonnage AS QTY,
                    'WELL TO WELL' AS Description,
                    v.Variant
                FROM summary_well_to_well_df s
                JOIN variant_map v ON v.overtime = s.overtime
        """,
        output=False
    )
    return (well_to_well_bc,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    --- Net List
    """)


@app.cell(hide_code=True)
def _(summary_net_list_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    net_list_bc = mo.sql(
        """
        WITH
                    service_map AS (
                        SELECT * FROM (
                            VALUES
                                ('Unload to Quay', 'Brine', 'UNLOAD TO QUAY - BRINE'),
                                ('Unload to Quay', 'Dry', 'UNLOAD TO QUAY - DRY'),
                                ('Unload to CCCS', 'Brine', 'UNLOAD TO COLD STORE - BRINE'),
                                ('Unload to CCCS', 'Dry', 'UNLOAD TO COLD STORE - DRY'),
                                ('Transhipment', 'Brine', 'TRANSHIPMENT - BRINE'),
                                ('Transhipment', 'Dry', 'TRANSHIPMENT - DRY'),
                                ('Basic OSS', 'Brine', 'STO -BASIC OSS - BRINE'),
                                ('Basic OSS', 'Dry', 'STO -BASIC OSS - DRY'),
                                ('Container Stuffing', 'Brine', 'CONTAINER STUFFING - BRINE'),
                                ('Container Stuffing', 'Dry', 'CONTAINER STUFFING - DRY'),
                                ('Full OSS', 'Brine', 'CONTAINER STUFFING - BRINE'),
                                ('Full OSS', 'Dry', 'CONTAINER STUFFING - DRY')
                        ) AS t (service, storage_type, Description)
                    ),
                    variant_map AS (
                        SELECT * FROM (
                            VALUES
                                ('normal hours', 'STD'),
                                ('overtime 150%', '150%'),
                                ('overtime 200%', '200%')
                        ) AS t (overtime, Variant)
                    )
                SELECT
                    s.total_tonnage AS QTY,
                    m.Description,
                    v.Variant
                FROM summary_net_list_df s
                JOIN service_map m
                    ON m.service = s.service
                    AND m.storage_type = s.storage_type
                JOIN variant_map v ON v.overtime = s.overtime
        """,
        output=False
    )
    return (net_list_bc,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    --- Shore Crane
    """)


@app.cell(hide_code=True)
def _(summary_shore_crane_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    shore_crane_bc = mo.sql(
        """
        WITH variant_map AS (
                    SELECT * FROM (
                        VALUES
                            ('Normal Hours', 'STD'),
                            ('Overtime 150%', 'OT1'),
                            ('Overtime 200%', 'OT2')
                    ) AS t (overtime, Variant)
                )
                SELECT
                    s.value AS QTY,
                    'SHORE CRANE RENTAL' AS Description,
                    v.Variant
                FROM summary_shore_crane_df s
                JOIN variant_map v ON v.overtime = s.overtime
        """,
        output=False
    )
    return (shore_crane_bc,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    --- Transfer
    """)


@app.cell(hide_code=True)
def _(summary_transfer_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    transfer_bc = mo.sql(
        """
        WITH variant_map AS (
                    SELECT * FROM (
                        VALUES
                            ('Normal Hours', 'STD'),
                            ('Overtime 150%', '150%'),
                            ('Overtime 200%', '200%')
                    ) AS t (overtime, Variant)
                )
                SELECT
                    s.total_service AS QTY,
                    'TRANSFER FEU' AS Description,
                    v.Variant
                FROM summary_transfer_df s
                JOIN variant_map v ON v.overtime = s.overtime
        """,
        output=False
    )
    return (transfer_bc,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.md(r"""
    ## Combined Summaries
    """)


@app.cell
def _(additional_overtime_bc, berth_bc, extra_men_bc, forklift_bc, net_list_bc, salt_bc, shore_crane_bc, stuffing_bc, tare_bc, transfer_bc, well_to_well_bc, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    combined_services_bc = pl.concat(
        [
            _df.select(
                pl.col("QTY").cast(pl.Decimal(scale=3)),
                pl.col("Description").cast(pl.Utf8),
                pl.col("Variant").cast(pl.Utf8),
            )
            for _df in [
                berth_bc,
                tare_bc,
                stuffing_bc,
                salt_bc,
                forklift_bc,
                extra_men_bc,
                additional_overtime_bc,
                well_to_well_bc,
                net_list_bc,
                shore_crane_bc,
                transfer_bc,
            ]
        ]
    )
    combined_services_bc
    return (combined_services_bc,)


@app.cell(hide_code=True)
def _(combined_services_bc, customer, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    pricing_df = mo.sql(
        f"""
        WITH
            bc AS (
                SELECT
                    "Type",
                    "No.",
                    "Description",
                    "Variant",
                    "Unit Price",
                    list_position(
                        [
                            'BERTH DUES (FISHING VESSELS)',
                            'BERTH DUES (LONGLINER)',
                            'BERTH DUES (CARGO)',
                            'BERTH DUES (SUPPLY VESSELS)',
                            'BERTH DUES (MILITARY)',
                            'TARE- RENTAL',
                            'CALIBRATION',
                            'PLUGIN',
                            'MONITORING',
                            'ELECTRICITY - 25°',
                            'ELECTRICITY - 35°',
                            'ELECTRICITY - 60°',
                            'SALT LOADING',
                            'FORKLIFT RENTAL',
                            'EXTRA MEN',
                            'ADDITIONAL OVERTIME',
                            'WELL TO WELL',
                            'UNLOAD TO COLD STORE - DRY',
                            'UNLOAD TO COLD STORE - BRINE',
                            'TRANSHIPMENT - DRY',
                            'TRANSHIPMENT - BRINE',
                            'STO -BASIC OSS - DRY',
                            'STO -BASIC OSS - BRINE',
                            'UNLOAD TO QUAY - DRY',
                            'UNLOAD TO QUAY - BRINE',
                            'CONTAINER STUFFING - DRY',
                            'CONTAINER STUFFING - BRINE',
                            'SHORE CRANE RENTAL',
                            'TRANSFER FEU'
                        ],
                        "Description"
                    ) AS sort_order
                FROM
                    price_df
                WHERE
                    "Description" IN (
                        'BERTH DUES (FISHING VESSELS)',
                        'BERTH DUES (LONGLINER)',
                        'BERTH DUES (CARGO)',
                        'BERTH DUES (SUPPLY VESSELS)',
                        'BERTH DUES (MILITARY)',
                        'TARE- RENTAL',
                        'CALIBRATION',
                        'PLUGIN',
                        'MONITORING',
                        'ELECTRICITY - 25°',
                        'ELECTRICITY - 35°',
                        'ELECTRICITY - 60°',
                        'SALT LOADING',
                        'FORKLIFT RENTAL',
                        'EXTRA MEN',
                        'ADDITIONAL OVERTIME',
                        'WELL TO WELL',
                        'UNLOAD TO COLD STORE - DRY',
                        'UNLOAD TO COLD STORE - BRINE',
                        'TRANSHIPMENT - DRY',
                        'TRANSHIPMENT - BRINE',
                        'STO -BASIC OSS - DRY',
                        'STO -BASIC OSS - BRINE',
                        'UNLOAD TO QUAY - DRY',
                        'UNLOAD TO QUAY - BRINE',
                        'CONTAINER STUFFING - DRY',
                        'CONTAINER STUFFING - BRINE',
                        'SHORE CRANE RENTAL',
                        'TRANSFER FEU'
                    )
            ),

            summaries AS (
                FROM
                    combined_services_bc
                SELECT
                    *
                WHERE
                    QTY <> 0
            )

        SELECT
            b."Type",
            b."No.",
            b."Description",
            b."Variant",
            s.QTY::DECIMAL AS QTY,
            CASE
                WHEN b."Description" = 'BERTH DUES (FISHING VESSELS)'
                    AND '{customer.value}' IN (
                        'ECHEBASTAR',
                        'HARTSWATER LIMITED'
                    )
                THEN 1250
                ELSE b."Unit Price"::DECIMAL
            END AS "Unit Price"
        FROM
            bc AS b
        LEFT JOIN summaries AS s
            ON s."Description" = b."Description"
            AND s."Variant" = b."Variant"
        WHERE
            s.QTY IS NOT NULL
        ORDER BY
            b.sort_order,
            b."Variant";
        """
    )
    return (pricing_df,)


@app.cell(hide_code=True)
def _(service_breakdown_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    metrics_df = mo.sql(
        """
        WITH
            a AS (
                FROM
                    service_breakdown_df
                SELECT
                    Description,
                    Variant,
                    "unit_price",
                    quantity AS QTY,
                    total AS total_price
            )
        FROM
            a
        SELECT SUM(total_price) AS total_price
        """
    )
    return (metrics_df,)


@app.cell
def _(mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    return


@app.cell(hide_code=True)
def _(pricing_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    bc_df = mo.sql(
        """
        FROM pricing_df
            SELECT * EXCLUDE("Unit Price")
        """
    )
    return (bc_df,)


@app.cell(hide_code=True)
def _(discount_berthing_figure: int, pricing_df, value_berthing_figure: int, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    service_breakdown_df = mo.sql(
        f"""
        FROM pricing_df
                SELECT
                    'STO Services' AS section,
            Description,
            Variant,
                    Description ||
                        CASE WHEN Variant <> 'STD' THEN ' (' || Variant || ')' ELSE '' END
                        AS service,
                    ROW_NUMBER() OVER (ORDER BY Description, Variant) AS sort_order,
                    "Unit Price" AS unit_price,
                    QTY AS quantity,
                    CASE WHEN service LIKE '%BERTH%' AND QTY > 0 THEN '{value_berthing_figure}' WHEN service LIKE '%BERTH%' AND QTY < 0 THEN '{discount_berthing_figure}' ELSE ROUND(QTY * "Unit Price", 2) END AS total

                ORDER BY sort_order
        """
    )
    return (service_breakdown_df,)


@app.cell
def _(additional_overtime_df, days_on_electricity, end_date, extra_men_df, final_berth_df, final_forklift_df, net_list_df, no_of_plugin, salt_df, select_report, service_breakdown_df, shore_crane_df, start_date, stuffing_df, tare_df, transfer_df, well_to_well_df, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    _detail_sheets = {
        "Berth Dues": final_berth_df,
        "Tare Weight": tare_df,
        "Stuffing": stuffing_df,
        "Salt": salt_df,
        "Forklift": final_forklift_df,
        "Extra Men": extra_men_df,
        "Additional Overtime": additional_overtime_df,
        "Well to Well": well_to_well_df,
        "Net List": net_list_df,
        "Shore Crane": shore_crane_df,
        "Transfer": transfer_df,
    }

    reports = {
        "Summary": {
            "type": "summary",
            "month": f"{start_date.value} to {end_date}",
            "client": f"{select_report.value}",
            "summary_rows": [
                None,  # blank row
                {
                    "label": "Plugin:",
                    "value": no_of_plugin,
                    "unit": "Plugins",
                    "italic": True,
                },
                {
                    "label": "Electricity:",
                    "value": days_on_electricity,
                    "unit": "Days",
                    "italic": True,
                },
                None,
            ],
            # Expected columns:
            # section, service, quantity, unit_price, total
            "service_df": service_breakdown_df,
            "landscape": False,
            "pages_wide": 1,
            "pages_tall": 1,
        },
        # Only export detail sheets that actually have rows.
        **{
            _name: {"df": _df, "header_color": "#1f6fb2"}
            for _name, _df in _detail_sheets.items()
            if len(_df) > 0
        },
    }
    return (reports,)


@app.cell
def _(reports, save_button, select_report, end_date, mode):
    mo.stop(mode.value not in ("STO", "Extended OSS"))
    mo.stop(
        not save_button.value,
        mo.md("*Click the button to generate the file.*"),
    )

    output_file = export_dataframes(
        dataframes=reports,
        output_path=f"output/{select_report.value} {mode.value} - {format_datestr_to_month_year(str(end_date))}.xlsx",
    )
    mo.md(f"✅ Saved to `{output_file}`")


@app.cell
def _(report_status_df, select_report, mode):
    mo.stop(mode.value != "OSS")
    _options = (
        report_status_df.filter(pl.col("vessel/client").eq(select_report.value))
        .select(pl.col("start_date").dt.date().dt.strftime(format="%Y-%m-%d"))
        .unique()
        .sort("start_date")
            .collect()
        .to_series()
        .to_list()
    )

    oss_start_date = mo.ui.dropdown(
        options=_options,
        value=_options[0] if _options else None,
        label="Start Date",
    )
    return (oss_start_date,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value != "OSS")
    mo.md(r"""
 
    """)


@app.cell
def _(oss_copy_button, oss_customer, oss_document_date, oss_metrics_df, oss_period_end, oss_period_start, oss_posting_date, oss_save_button, select_report, oss_start_date, oss_sto_number, oss_summary_table, mode):
    mo.stop(mode.value != "OSS")
    oss_title = mo.md("# ::lucide:blocks:: OSS Report")

    oss_filter_bar = mo.hstack(
        [
            select_report,
            oss_start_date,
        ],
        justify="start",
        gap=2,
    )

    oss_meta = mo.hstack(
        [
            mo.stat(
                label="Invoice Value",
                value=f"{oss_metrics_df.item():,.2f}",
                caption="Total price (USD)",
            ),
        ],
        justify="start",
        gap=1,
    )

    oss_actions = mo.hstack([oss_copy_button, oss_save_button], justify="start", gap=1)

    mo.vstack(
        [
            oss_title,
            mo.md("---"),
            oss_filter_bar,
            oss_customer,
            oss_meta,
            mo.md("---"),
            oss_document_date,
            oss_posting_date,
            oss_period_start,
            oss_period_end,
            oss_sto_number,
            oss_summary_table,
            mo.md("---"),
            oss_actions,
        ]
    )


@app.cell(hide_code=True)
def _(report_status_df, select_report, oss_start_date, mode):
    mo.stop(mode.value != "OSS")
    oss_report_data_df = mo.sql(
        f"""
        WITH

            report AS (
                FROM
                    report_status_df
                SELECT
                    "month" AS report_month,
                    sub_type AS sto_number,
                    "vessel/client" AS vessel,
                    customer,
                    start_date,
                    end_date
                WHERE
                    report_type = 'OSS'
                    AND "vessel/client" = '{select_report.value}'
                    AND start_date = '{oss_start_date.value}'
            )


        FROM
            report r
        """
    )
    return (oss_report_data_df,)


@app.cell
def _(oss_report_data_df, mode):
    mo.stop(mode.value != "OSS")
    oss_end_date = get_first_value(oss_report_data_df, "end_date", default="")
    return (oss_end_date,)


@app.cell
def _(oss_report_data_df, mode):
    mo.stop(mode.value != "OSS")
    oss_document_date = mo.ui.text(
        label="Document Date:",
        value=str(
            get_first_value(oss_report_data_df, "end_date", default="")
        ),
    )

    oss_posting_date = mo.ui.text(
        label="Posting Date:",
        value=get_first_value(
            oss_report_data_df,
            "end_date",
            default="",
        ),
    )

    oss_period_start = mo.ui.text(
        label="Period Start:",
        value=str(get_first_value(oss_report_data_df, "start_date", default=""))
    )

    oss_period_end = mo.ui.text(
        label="Period End:",
        value=str(get_first_value(oss_report_data_df, "end_date", default="")),
    )

    oss_sto_number = mo.ui.text(
        label="StopOver No.",
        value=str(get_first_value(oss_report_data_df, "sto_number", default="")),
    )

    oss_customer = mo.ui.text(
        label="Customer Name",
        value=str(get_first_value(oss_report_data_df, "customer", default="")),
    )
    return (
        oss_customer,
        oss_document_date,
        oss_period_end,
        oss_period_start,
        oss_posting_date,
        oss_sto_number,
    )


@app.cell
def _(mode):
    mo.stop(mode.value != "OSS")
    oss_copy_button = mo.ui.run_button(label="📋 Copy BC data to clipboard")
    oss_save_button = mo.ui.run_button(label="💾 Save XLSX report")
    return oss_copy_button, oss_save_button


@app.cell(hide_code=True)
def _(oss_days_on_electricity, oss_no_of_plugin, mode):
    mo.stop(mode.value != "OSS")
    oss_summary_table = mo.Html(
        f"""
        <table style="
            margin: 0 auto;
            border-collapse: collapse;
            font-size: 16px;
            line-height: 1.45;
        ">
            <tr>
                <td style="text-align:right; padding-right:28px; font-style:italic;">
                    Plugin
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {oss_no_of_plugin}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Plugins
                </td>
            </tr>
            <tr>
                <td style="text-align:right; padding-right:28px; font-style:italic;">
                    Electricity
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {oss_days_on_electricity}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Days
                </td>
            </tr>
            <tr>
                <td colspan="3" style="height:18px;"></td>
            </tr>
        </table>
        """
    )
    return (oss_summary_table,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value != "OSS")
    mo.md(r"""
    ## Datasets
    """)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value != "OSS")
    mo.md(r"""
    ### Stuffing ::lucide:container::
    """)


@app.cell(hide_code=True)
def _(oss_end_date, full_oss_dataset, select_report, oss_start_date, mode):
    mo.stop(mode.value != "OSS")
    oss_stuffing_old_df = mo.sql(
        f"""
        FROM FULL_OSS_DATASET
        WHERE vessel = '{select_report.value}' AND date BETWEEN '{oss_start_date.value}' AND '{oss_end_date}'
        ORDER BY date
        """
    )
    return (oss_stuffing_old_df,)


@app.cell(hide_code=True)
def _(oss_stuffing_old_df, mode):
    mo.stop(mode.value != "OSS")
    _df = mo.sql(
        """
        FROM stuffing_old_df
        SELECT storage_type,overtime,SUM(total_tonnage)::DECIMAL AS tonnage,SUM(invoice_value)::DECIMAL AS invoice_value
        GROUP BY ALL
        """
    )


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value != "OSS")
    mo.md(r"""
    ### Electricity ::lucide:plug-zap::
    """)


@app.cell(hide_code=True)
def _(oss_end_date, select_report, oss_start_date, stuffing_dataset, mode):
    mo.stop(mode.value != "OSS")
    stuffing_i_df = mo.sql(
        f"""
        FROM
            STUFFING_DATASET
        SELECT
            vessel_client AS vessel,
            date_plugged::DATE + time_plugged::TIME AS datetime_plugged_in,
            container_number,shipping_line,
            plugged_status,
            tonnage,
            set_point,
            date_out,
            CASE
                WHEN operation_type NOT LIKE '%OSS%' THEN 0
                ELSE days_on_plug
            END AS days_on_plug,
            CASE
                WHEN operation_type NOT LIKE '%OSS%' THEN 0
                ELSE total
            END AS total_price,
            CASE
                WHEN customer = 'IOT' THEN 'On the account of IOT'
                ELSE ''
            END AS remarks
        WHERE
            vessel_client = '{select_report.value}'
            AND date_plugged BETWEEN '{oss_start_date.value}' AND '{oss_end_date}'
        ORDER BY datetime_plugged_in
        """,
        output=False
    )


@app.cell(hide_code=True)
def _(oss_end_date, select_report, oss_start_date, stuffing_dataset, mode):
    mo.stop(mode.value != "OSS")
    oss_stuffing_df = mo.sql(
        f"""
        FROM STUFFING_DATASET
            SELECT date_plugged::DATE + time_plugged::TIME AS datetime_plugged_in,
            container_number,
            plugged_status,
            set_point,
            tonnage,
            date_out,
            days_on_plug,
            plugin_price,
            electricity_unit_price AS storage_price,
            monitoring_price,
            total_electricity,
            total AS total_price,
            CASE WHEN location = 'LML' THEN '' ELSE location END  AS remarks

        WHERE
            customer = 'MAERSKLINE' AND
            vessel_client = '{select_report.value}'
            AND date_plugged BETWEEN '{oss_start_date.value}' AND '{oss_end_date}'
        """
    )
    return (oss_stuffing_df,)


@app.cell
def _(oss_stuffing_df, mode):
    mo.stop(mode.value != "OSS")
    # STO summary figures shown in the UI and on the Summary sheet.
    oss_no_of_plugin = oss_stuffing_df.filter(pl.col("remarks").is_null()).height
    oss_days_on_electricity = (
        oss_stuffing_df.filter(pl.col("remarks").is_null())
        .get_column("days_on_plug")
        .sum()
        or 0
    )
    return oss_days_on_electricity, oss_no_of_plugin


@app.cell(hide_code=True)
def _(oss_stuffing_df, mode):
    mo.stop(mode.value != "OSS")
    oss_plugin_summary_df = mo.sql(
        """
        WITH
            plugin AS (
                FROM
                    stuffing_df
                SELECT
                    'PLUGIN' AS service,
                    COUNT_IF(plugin_price = 25) AS Number,
                    SUM(plugin_price) AS price
            ),
            monitoring AS (
                FROM
                    stuffing_df
                SELECT
                    'MONITORING' AS service,
                    COUNT_IF(monitoring_price = 30) AS Number,
                    SUM(monitoring_price) AS price
            ),
            electricity_25 AS (
                FROM
                    stuffing_df
                SELECT
                    'ELECTRICITY -25°' AS service,
                    COALESCE(SUM(days_on_plug), 0) AS Number,
                    COALESCE(SUM(total_electricity), 0) AS price
                WHERE
                    set_point = -25
            ),
            electricity_35 AS (
                FROM
                    stuffing_df
                SELECT
                    'ELECTRICITY -35°' AS service,
                    COALESCE(SUM(days_on_plug), 0) AS Number,
                    COALESCE(SUM(total_electricity), 0) AS price
                WHERE
                    set_point = -35
            ),
            electricity_60 AS (
                FROM
                    stuffing_df
                SELECT
                    'ELECTRICITY -60°' AS service,
                    COALESCE(SUM(days_on_plug), 0) AS Number,
                    COALESCE(SUM(total_electricity), 0) AS price
                WHERE
                    set_point = -60
            )
        FROM
            plugin
        UNION ALL
        FROM
            monitoring
        UNION ALL
        FROM
            electricity_25
        UNION ALL
        FROM
            electricity_35
        UNION ALL
        FROM
            electricity_60
        """
    )
    return (oss_plugin_summary_df,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value != "OSS")
    mo.md(r"""
    ### Net List
    """)


@app.cell(hide_code=True)
def _(oss_end_date, full_oss_dataset, select_report, oss_start_date, mode):
    mo.stop(mode.value != "OSS")
    oss_net_list_df = mo.sql(
        f"""
        FROM FULL_OSS_DATASET
        WHERE
            vessel = '{select_report.value}'
            AND date BETWEEN '{oss_start_date.value}' AND '{oss_end_date}'
        """
    )
    return (oss_net_list_df,)


@app.cell(hide_code=True)
def _(oss_net_list_df, mode):
    mo.stop(mode.value != "OSS")
    oss_summary_net_list_df = mo.sql(
        """
        FROM net_list_df
        SELECT service,storage_type,overtime,SUM(total_tonnage)::DECIMAL AS total_tonnage,SUM(invoice_value)::DECIMAL AS total_price
        GROUP BY ALL
        ORDER BY service,overtime
        """
    )
    return (oss_summary_net_list_df,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value != "OSS")
    mo.md(r"""
    ### Shore Crane ::lucide:git-pull-request-create::
    """)


@app.cell(hide_code=True)
def _(oss_end_date, select_report, shore_crane_dataset, oss_start_date, mode):
    mo.stop(mode.value != "OSS")
    oss_shore_crane_df = mo.sql(
        f"""
        FROM SHORE_CRANE_DATASET
        WHERE
            customer = '{select_report.value}' AND invoiced_to = 'MAERSKLINE'
            AND date BETWEEN '{oss_start_date.value}' AND '{oss_end_date}'
        """
    )
    return (oss_shore_crane_df,)


@app.cell(hide_code=True)
def _(oss_shore_crane_df, mode):
    mo.stop(mode.value != "OSS")
    oss_summary_shore_crane_df = mo.sql(
        """
        WITH
            ot_2 AS (
                FROM
                    shore_crane_df
                SELECT
                    'Overtime 200%' AS overtime,
                    SUM(overtime_hours) AS value
                WHERE
                    day_name IN ('Sun', 'PH')
            ),
            ot_1_sun AS (
                FROM
                    shore_crane_df
                SELECT
                    'Overtime 150%' AS overtime,
                    SUM(hours - overtime_hours) AS value
                WHERE
                    day_name IN ('Sun', 'PH')
            ),
            ot_1 AS (
                FROM
                    shore_crane_df
                SELECT
                    'Overtime 150%' AS overtime,
                    SUM(overtime_hours) AS value
                WHERE
                    day_name NOT IN ('Sun', 'PH')
            ),
            normal AS (
                FROM
                    shore_crane_df
                SELECT
                    'Normal Hours' AS overtime,
                    SUM(hours - overtime_hours) AS value
                WHERE
                    day_name NOT IN ('Sun', 'PH')
            ),
            -- Append the data
            grouped AS (
                FROM
                    normal
                UNION ALL
                FROM
                    ot_1
                UNION ALL
                FROM
                    ot_1_sun
                UNION ALL
                FROM
                    ot_2
            ),
            main AS (
                FROM
                    grouped
                SELECT
                    overtime,
                    SUM(value) AS value
                GROUP BY ALL
            ),
            sort_order AS (
                FROM
                    main
                SELECT
                    overtime,
                    CASE
                        WHEN overtime = 'Normal Hours' THEN 1
                        WHEN overtime = 'Overtime 150%' THEN 2
                        WHEN overtime = 'Overtime 200%' THEN 3
                        ELSE 0
                    END AS sort
            ),
            unit_price AS (
                FROM
                    main
                SELECT
                    overtime,
                    CASE
                        WHEN overtime = 'Normal Hours' THEN 135 * 1.0
                        WHEN overtime = 'Overtime 150%' THEN 135 * 1.6
                        WHEN overtime = 'Overtime 200%' THEN 135 * 2.1
                        ELSE 0
                    END AS unit_price
            )
        SELECT
            m.*,
            p.unit_price * m.value AS total_price
        FROM
            main m
            LEFT JOIN sort_order s ON s.overtime = m.overtime
            LEFT JOIN unit_price p ON p.overtime = m.overtime
        ORDER BY
            s.sort
        """
    )
    return (oss_summary_shore_crane_df,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value != "OSS")
    mo.md(r"""
    ### Transfer
    """)


@app.cell
def _(mode):
    mo.stop(mode.value != "OSS")
    oss_affectation_df = pl.read_excel(
        source=VALIDATION_XLSX,
        table_name="Affectation",
    ).pipe(clean_affectation)
    return (oss_affectation_df,)


@app.cell(hide_code=True)
def _(oss_affectation_df, oss_end_date, select_report, oss_start_date, mode):
    mo.stop(mode.value != "OSS")
    oss_to_filter_transfer_df = mo.sql(
        f"""
        FROM affectation_df
        WHERE vessel = '{select_report.value}' AND date BETWEEN '{oss_start_date.value}' AND '{oss_end_date}'
        """,
        output=False
    )
    return (oss_to_filter_transfer_df,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value != "OSS")
    mo.md(r"""
    #### Container still on plug
    """)


@app.cell
def _(oss_to_filter_transfer_df, mode):
    mo.stop(mode.value != "OSS")
    container_on_plug = oss_to_filter_transfer_df.filter(
        pl.col("exit_date").is_null()
    )
    container_on_plug


@app.cell(hide_code=True)
def _(oss_to_filter_transfer_df, transfer_dataset, mode):
    mo.stop(mode.value != "OSS")
    oss_transfer_df = mo.sql(
        """
        WITH
            t AS (
                FROM
                    TRANSFER_DATASET
                WHERE
                    status = 'Full'
            )
        SELECT
            t.day_name,
            t.container_number,
            t.date,
            t.movement_type,
            t.destination,
            t.status,
            t.time_out::TIME AS time_out,
            CASE
                WHEN driver LIKE '%IPHS%' THEN 'IPHS'
                ELSE driver
            END AS driver,
            t."type",
            t.size,
            f.vessel,
            t.haulage_price AS price_per_move,
            t.remarks
        FROM
            t
            LEFT JOIN to_filter_transfer_df f ON f.exit_date = t.date
            AND f.container_number = t.container_number
        WHERE
            f.exit_date IS NOT NULL
           AND t.remarks = 'MAERSKLINE'
        """
    )
    return (oss_transfer_df,)


@app.cell(hide_code=True)
def _(oss_transfer_df, mode):
    mo.stop(mode.value != "OSS")
    oss_summary_transfer_df = mo.sql(
        """
        WITH
            main AS (
                FROM
                    transfer_df
                SELECT
                    COUNT(*) AS total_service,
                    CASE
                        WHEN day_name IN ('Sun', 'PH')
                        AND time_out::TIME > '16:00'::TIME THEN 'Overtime 200%'
                        WHEN (
                            day_name IN ('Sun', 'PH')
                            OR time_out::TIME > '17:00'::TIME
                        )
                        OR (
                            time_out::TIME BETWEEN '00:00'::TIME AND '08:00'::TIME
                        ) THEN 'Overtime 150%'
                        ELSE 'Normal Hours'
                    END AS overtime
                GROUP BY ALL
            ),
            sort_order AS (
                SELECT
                    *
                FROM
                    (
                        VALUES
                            ('Normal Hours', 1),
                            ('Overtime 150%', 2),
                            ('Overtime 200%', 3)
                    ) AS t (overtime, sort)
            ),
            unit_price AS (
                SELECT
                    *
                FROM
                    (
                        VALUES
                            ('Normal Hours', 90),
                            ('Overtime 150%', 90 * 1.5),
                            ('Overtime 200%', 90 * 2.0)
                    ) AS t (overtime, unit_price)
            )
        SELECT
            m.overtime,
            COALESCE(o.total_service, 0) AS total_service,
            p.unit_price * COALESCE(o.total_service, 0) AS total_price
        FROM
            sort_order m
            FULL OUTER JOIN main o ON o.overtime = m.overtime
            LEFT JOIN unit_price p ON p.overtime = m.overtime
        ORDER BY
            m.sort
        """
    )
    return (oss_summary_transfer_df,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value != "OSS")
    mo.md(r"""
    ## Summaries Df (Business Central line items)
    """)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value != "OSS")
    mo.md(r"""
    -- Stuffing
    """)


@app.cell(hide_code=True)
def _(oss_plugin_summary_df, mode):
    mo.stop(mode.value != "OSS")
    oss_stuffing_bc = mo.sql(
        """
        WITH
            plugin AS (
                FROM
                    plugin_summary_df
                SELECT
                   Number AS QTY,
                    service AS Description,
                    'STD' AS Variant
                WHERE
                    service = 'PLUGIN'
            ),
            monitoring AS (
               FROM
                    plugin_summary_df
                SELECT
                   Number AS QTY,
                    service AS Description,
                    'STD' AS Variant
                WHERE
                    service = 'MONITORING'
            ),
            standard AS (
                       FROM
                    plugin_summary_df
                SELECT
                   Number AS QTY,
                    'ELECTRICITY - 25°' AS Description,
                    'STD' AS Variant
                WHERE
                    service = 'ELECTRICITY -25°'
            ),
            magnum AS (
                FROM
                    plugin_summary_df
                SELECT
                   Number AS QTY,
                    'ELECTRICITY - 35°' AS Description,
                    'STD' AS Variant
                WHERE
                    service = 'ELECTRICITY -35°'
            ),
            s_freezer AS (
               FROM
                    plugin_summary_df
                SELECT
                   Number AS QTY,
                    'ELECTRICITY - 60°' AS Description,
                    'STD' AS Variant
                WHERE
                    service = 'ELECTRICITY -60°'
            )
        FROM
            plugin
        UNION ALL
        FROM
            monitoring
        UNION ALL
        FROM
            standard
        UNION ALL
        FROM magnum
        UNION ALL
        FROM s_freezer
        """
    )
    return (oss_stuffing_bc,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value != "OSS")
    mo.md(r"""
    --- Net List
    """)


@app.cell(hide_code=True)
def _(oss_summary_net_list_df, mode):
    mo.stop(mode.value != "OSS")
    oss_net_list_bc = mo.sql(
        """
        WITH
            service_map AS (
                SELECT
                    *
                FROM
                    (
                        VALUES
                            ('Stuffing', 'Brine', 'BASIC OSS - BRINE'),
                            ('Stuffing', 'Dry', 'BASIC OSS - DRY'),
                            ('Container Stuffing - Brine', 'Brine', 'FULL OSS - BRINE'),
                            ('Container Stuffing - Dry', 'Dry', 'FULL OSS - DRY')
                    ) AS t (service, storage_type, Description)
            ),
            variant_map AS (
                SELECT
                    *
                FROM
                    (
                        VALUES
                            ('normal hours', 'STD'),
                            ('overtime 150%', '150%'),
                            ('overtime 200%', '200%')
                    ) AS t (overtime, Variant)
            )
        SELECT
            s.total_tonnage AS QTY,
            m.Description,
            v.Variant
        FROM
            summary_net_list_df s
            JOIN service_map m ON m.service = s.service
            AND m.storage_type = s.storage_type
            JOIN variant_map v ON v.overtime = s.overtime
        """
    )
    return (oss_net_list_bc,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value != "OSS")
    mo.md(r"""
    --- Shore Crane
    """)


@app.cell(hide_code=True)
def _(oss_summary_shore_crane_df, mode):
    mo.stop(mode.value != "OSS")
    oss_shore_crane_bc = mo.sql(
        """
        WITH variant_map AS (
                    SELECT * FROM (
                        VALUES
                            ('Normal Hours', 'STD'),
                            ('Overtime 150%', 'OT1'),
                            ('Overtime 200%', 'OT2')
                    ) AS t (overtime, Variant)
                )
                SELECT
                    s.value AS QTY,
                    'SHORE CRANE RENTAL' AS Description,
                    v.Variant
                FROM summary_shore_crane_df s
                JOIN variant_map v ON v.overtime = s.overtime
        """,
        output=False
    )
    return (oss_shore_crane_bc,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value != "OSS")
    mo.md(r"""
    --- Transfer
    """)


@app.cell(hide_code=True)
def _(oss_summary_transfer_df, mode):
    mo.stop(mode.value != "OSS")
    oss_transfer_bc = mo.sql(
        """
        WITH variant_map AS (
                    SELECT * FROM (
                        VALUES
                            ('Normal Hours', 'STD'),
                            ('Overtime 150%', '150%'),
                            ('Overtime 200%', '200%')
                    ) AS t (overtime, Variant)
                )
                SELECT
                    s.total_service AS QTY,
                    'TRANSFER FEU' AS Description,
                    v.Variant
                FROM summary_transfer_df s
                JOIN variant_map v ON v.overtime = s.overtime
        """,
        output=False
    )
    return (oss_transfer_bc,)


@app.cell(hide_code=True)
def _(mode):
    mo.stop(mode.value != "OSS")
    mo.md(r"""
    ## Combined Summaries
    """)


@app.cell
def _(oss_net_list_bc, oss_shore_crane_bc, oss_stuffing_bc, oss_transfer_bc, mode):
    mo.stop(mode.value != "OSS")
    oss_combined_services_bc = pl.concat(
        [
            _df.select(
                pl.col("QTY").cast(pl.Decimal(scale=3)),
                pl.col("Description").cast(pl.Utf8),
                pl.col("Variant").cast(pl.Utf8),
            )
            for _df in [


                oss_stuffing_bc,


                oss_net_list_bc,
                oss_shore_crane_bc,
                oss_transfer_bc,
            ]
        ]
    )
    oss_combined_services_bc
    return (oss_combined_services_bc,)


@app.cell(hide_code=True)
def _(oss_combined_services_bc, mode):
    mo.stop(mode.value != "OSS")
    oss_pricing_df = mo.sql(
        """
        WITH bc AS (FROM
            price_df
            SELECT 
            "Type",
            "No.",
            "Description",
            "Variant",
           "Unit Price"

        WHERE
            Description IN (

            'TRANSFER FEU',
            'SHORE CRANE RENTAL',
            'BASIC OSS - BRINE',
            'BASIC OSS - DRY',
            'FULL OSS - BRINE',
            'FULL OSS - DRY',
            'PLUGIN',
            'MONITORING',
            'ELECTRICITY - 25°',
        	'ELECTRICITY - 35°',
        	'ELECTRICITY - 60°',




            ) ),summaries AS (FROM combined_services_bc WHERE QTY <>0)


        SELECT 
            b.Type,
            b."No.",
            b.Description,
            b.Variant,
            s.QTY::DECIMAL AS QTY,
            CASE WHEN b.Description = 'BERTH DUES (FISHING VESSELS)' THEN 1000 ELSE b."Unit Price"::DECIMAL END AS "Unit Price"
        FROM bc b LEFT JOIN summaries s ON s.Description = b.Description AND s.Variant = b.Variant
        WHERE s.QTY IS NOT NULL AND s.QTY >0
        """
    )
    return (oss_pricing_df,)


@app.cell(hide_code=True)
def _(oss_pricing_df, mode):
    mo.stop(mode.value != "OSS")
    oss_metrics_df = mo.sql(
        """
        WITH a AS (FROM pricing_df
                SELECT Description, Variant, "Unit Price", QTY,
                    ROUND("Unit Price" * QTY, 2) AS total_price
                WHERE QTY > 0)

                FROM a
                SELECT COALESCE(ROUND(SUM(total_price)::FLOAT8, 2), 0) AS total_price
        """
    )
    return (oss_metrics_df,)


@app.cell
def _(oss_bc_df, oss_copy_button, mode):
    mo.stop(mode.value != "OSS")
    mo.stop(not oss_copy_button.value)
    oss_bc_df.write_clipboard()  # tab-separated, pastes cleanly into BC / Excel
    mo.md("✅ Copied to clipboard")


@app.cell(hide_code=True)
def _(oss_pricing_df, mode):
    mo.stop(mode.value != "OSS")
    oss_bc_df = mo.sql(
        """
        FROM pricing_df
            SELECT * EXCLUDE("Unit Price")
        WHERE QTY > 0
        """
    )
    return (oss_bc_df,)


@app.cell(hide_code=True)
def _(oss_pricing_df, mode):
    mo.stop(mode.value != "OSS")
    oss_service_breakdown_df = mo.sql(
        """
        FROM pricing_df
                SELECT
                    'STO Services' AS section,
                    Description ||
                        CASE WHEN Variant <> 'STD' THEN ' (' || Variant || ')' ELSE '' END
                        AS service,
                    ROW_NUMBER() OVER (ORDER BY Description, Variant) AS sort_order,
                    "Unit Price" AS unit_price,
                    QTY AS quantity,
                    ROUND(QTY * "Unit Price", 2) AS total
                WHERE QTY > 0
                ORDER BY sort_order
        """
    )
    return (oss_service_breakdown_df,)


@app.cell
def _(additional_overtime_df, oss_days_on_electricity, oss_end_date, extra_men_df, final_berth_df, final_forklift_df, oss_net_list_df, oss_no_of_plugin, salt_df, select_report, oss_service_breakdown_df, oss_shore_crane_df, oss_start_date, oss_stuffing_df, tare_df, oss_transfer_df, well_to_well_df, mode):
    mo.stop(mode.value != "OSS")
    _detail_sheets = {
        "Berth Dues": final_berth_df,
        "Tare Weight": tare_df,
        "Stuffing": oss_stuffing_df,
        "Salt": salt_df,
        "Forklift": final_forklift_df,
        "Extra Men": extra_men_df,
        "Additional Overtime": additional_overtime_df,
        "Well to Well": well_to_well_df,
        "Net List": oss_net_list_df,
        "Shore Crane": oss_shore_crane_df,
        "Transfer": oss_transfer_df,
    }

    oss_reports = {
        "Summary": {
            "type": "summary",
            "month": f"{oss_start_date.value} to {oss_end_date}",
            "client": f"{select_report.value}",
            "summary_rows": [
                None,  # blank row
                {
                    "label": "Plugin:",
                    "value": oss_no_of_plugin,
                    "unit": "Plugins",
                    "italic": True,
                },
                {
                    "label": "Electricity:",
                    "value": oss_days_on_electricity,
                    "unit": "Days",
                    "italic": True,
                },
                None,
            ],
            # Expected columns:
            # section, service, quantity, unit_price, total
            "service_df": oss_service_breakdown_df,
            "landscape": False,
            "pages_wide": 1,
            "pages_tall": 1,
        },
        # Only export detail sheets that actually have rows.
        **{
            _name: {"df": _df, "header_color": "#1f6fb2"}
            for _name, _df in _detail_sheets.items()
            if len(_df) > 0
        },
    }
    return (oss_reports,)


@app.cell
def _(oss_reports, oss_save_button, select_report, oss_end_date, mode):
    mo.stop(mode.value != "OSS")
    mo.stop(
        not oss_save_button.value,
        mo.md("*Click the button to generate the file.*"),
    )

    output_file = export_dataframes(
        dataframes=oss_reports,
        output_path=f"output/{select_report.value} {mode.value} - {format_datestr_to_month_year(str(oss_end_date))}.xlsx",
    )
    mo.md(f"✅ Saved to `{output_file}`")


if __name__ == "__main__":
    app.run()
