import marimo

__generated_with = "0.23.14"
app = marimo.App(width="columns")

with app.setup:
    from concurrent.futures import ThreadPoolExecutor
    from datetime import date
    from calendar import monthrange
    import polars as pl
    import marimo as mo

    from data import bc_items_lf
    from save import export_dataframes
    from datasets import emr
    from scan_google_sheet import scan_google_sheet

    with ThreadPoolExecutor(max_workers=5) as _pool:
        _f_pti = _pool.submit(lambda: emr.pti().collect())
        _f_washing = _pool.submit(lambda: emr.washing().collect())
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

    PTI_DATASET = _f_pti.result()
    WASHING_DATASET = _fix_time(_f_washing.result())
    price_df = _f_price.result()


@app.cell
def _():
    from datasets import miscellaneous_lf,forklift_scow_handling,scow_transfer

    return forklift_scow_handling, miscellaneous_lf, scow_transfer


@app.cell
def _(forklift_scow_handling, miscellaneous_lf, scow_transfer):
    misc_df = miscellaneous_lf()
    fl_df = forklift_scow_handling.forklift_scow_handling_lf()
    scow_df = scow_transfer()
    return fl_df, misc_df, scow_df


@app.cell
def _(scow_df):
    scow_df.filter(pl.col("status").eq("Full")).with_columns(
        time_selection=pl.when(pl.col("movement_type").eq("Collection"))
        .then(pl.col("time_in"))
        .otherwise(pl.col("time_out"))
    ).with_columns(
        overtime=pl.when(
            pl.col("day_name")
            .is_in(["Sun", "PH"])
            .and_(pl.col("time_selection").gt(pl.time(16, 0)))
        )
        .then(pl.lit("overtime 200%"))
        .when(
            (
                pl.col("day_name")
                .is_in(["Sun", "PH"])
                .and_(pl.col("time_selection").le(pl.time(16, 0)))
            ).or_(
                pl.col("day_name")
                .is_in(["Sun", "PH"])
                .not_()
                .and_(pl.col("time_selection").gt(pl.time(17, 0)))
            )
        )
        .then(pl.lit("overtime 150%"))
        .otherwise(pl.lit("normal hours"))
    ).collect()
    return


@app.cell(hide_code=True)
def _(fl_df):
    _df = mo.sql(
        f"""
        FROM fl_df
        """
    )
    return


@app.cell(hide_code=True)
def _(misc_df, month_selector):
    _df = mo.sql(
        f"""
        FROM misc_df
        WHERE operation_type LIKE '%Bin Dispatch%'  AND 
            date BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}')
        """
    )
    return


@app.function
def eomonth(dt: str) -> date:
    converted_date = date.fromisoformat(dt)
    last_day = monthrange(converted_date.year, converted_date.month)[1]
    return converted_date.replace(day=last_day)


@app.cell
def _():
    select_report = mo.ui.dropdown(
        label="Select Line",
        options=[
            "IOT",
        ],
        value="IOT",
    )

    _today = date.today()
    _years = sorted({_today.year, _today.year + 1})
    month_options = {
        date(_y, _m, 1).strftime("%b %Y"): f"{_y}-{_m:02d}-01"
        for _y in _years
        for _m in range(1, 13)
    }

    month_selector = mo.ui.dropdown(
        options=month_options,
        label="Select invoice month",
        value=_today.replace(day=1).strftime("%b %Y"),
    )
    return month_selector, select_report


@app.cell
def _(pti_df, washing_df):
    no_of_washing = washing_df.select(pl.len()).item()

    no_of_shifting = pti_df.select(pl.col("shifting_price").len()).item()

    no_of_plugin = pti_df.select(pl.col("plugin_price").len()).item()

    days_on_electricity = pti_df.select(
        pl.col("days_on_electricity").sum()
    ).item()
    return days_on_electricity, no_of_plugin, no_of_shifting, no_of_washing


@app.cell
def _():
    return


@app.cell
def _(days_on_electricity, no_of_plugin, no_of_shifting, no_of_washing):
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
                   Container Cleaning
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {no_of_washing}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Containers
                </td>
            </tr>

            <tr>
                <td style="text-align:right; padding-right:28px; font-style:italic;">
                    Shifting
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {no_of_shifting}
                </td>
                <td style="padding-left:12px; font-style:italic;">
                    Shifts
                </td>
            </tr>

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
                    Electricity
                </td>
                <td style="text-align:right; font-weight:bold;">
                    {days_on_electricity}
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
    return (summary_table,)


@app.cell
def _(
    copy_button,
    metrics_df,
    month_selector,
    save_button,
    select_report,
    summary_table,
):
    title = mo.md(f"# ⚙️🌊 IOT M&R Report")

    filter_bar = mo.hstack(
        [month_selector, select_report],
        justify="start",
        gap=2,
    )

    meta = mo.hstack(
        [
            mo.stat(
                label="Month",
                value=format_datestr_to_month_year(month_selector.value),
                caption="Operation date",
            ),
            mo.stat(
                label="Invoice Value",
                value=metrics_df.item(),
                caption="Total Price",
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
            meta,
            mo.md("---"),
            summary_table,
            mo.md("---"),
            actions,
        ]
    )
    return


@app.cell(hide_code=True)
def _(month_selector, pti_df, select_report, washing_df):
    _has_data = len(washing_df)+len(pti_df) > 0
    mo.callout(
        mo.md(
            f"**No records found** for **{select_report.value}** "
            f"in **{format_datestr_to_month_year(month_selector.value)}**. "
            "Check the vessel name or select a different month."
        ),
        kind="warn",
    ) if not _has_data else None
    return


@app.function
def format_datestr_to_month_year(date_str: str) -> str:
    """Format the datestring to month 'year"""
    date_converted = date.fromisoformat(date_str)
    month = date_converted.strftime(format="%B")
    year = date_converted.strftime(format="%y")
    return month + " '" + year


@app.cell(hide_code=True)
def _(pricing_df):
    metrics_df = mo.sql(
        f"""
        With a AS (FROM pricing_df

        SELECT Description,Variant,"Unit Price",QTY,ROUND("Unit Price" * QTY,2) AS total_price
        WHERE QTY > 0)

        FROM a 
         SELECT COALESCE(ROUND(SUM(total_price)::FLOAT8,2),0) AS total_price
        """
    )
    return (metrics_df,)


@app.cell(hide_code=True)
def _(month_selector, pti_dataset, select_report):
    pti_df = mo.sql(
        f"""
        FROM PTI_DATASET
            SELECT 
            datetime_start,
            container_number,
            set_point,
            invoice_to,
            datetime_end,
            status,
            "days" AS days_on_electricity,
            electricity_price,
            shifting_price,
            plugin_price,
            total_price

        WHERE
            invoice_to = '{select_report.value}'
           AND 
            datetime_start BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}')
        """
    )
    return (pti_df,)


@app.cell(hide_code=True)
def _(month_selector, select_report, washing_dataset):
    washing_df = mo.sql(
        f"""
        FROM WASHING_DATASET
            SELECT date AS cleaning_date,
            container_number,
            invoice_to AS client,
            price,
            CASE WHEN service_remarks = 'Clean' THEN '' ELSE service_remarks END AS service_remarks

        WHERE
            invoice_to = '{select_report.value}'
           AND 
            date BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}')
        ORDER BY date
        """
    )
    return (washing_df,)


@app.cell(hide_code=True)
def _(month_selector, select_report, washing_log_df):
    _df = mo.sql(
        f"""
        FROM washing_log_df WHERE "Invoice To" = '{select_report.value}'
         AND 
            date BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}')
        ORDER BY date
        """
    )
    return


@app.cell(hide_code=True)
def _(month_selector, pti_log_df, select_report):
    _df = mo.sql(
        f"""
        FROM pti_log_df WHERE "Shipping Line" = '{select_report.value}'
         AND 
            "Date Plugin" BETWEEN '{month_selector.value}' AND LAST_DAY(DATE '{month_selector.value}')
        ORDER BY "Date Plugin"
        """
    )
    return


@app.cell
def _():
    washing_log_df = scan_google_sheet(url="https://docs.google.com/spreadsheets/d/1L9qkq9WlIa2j5DcvoLvxkqYogRg76S-e8OxAIyLruAE/edit?gid=656582295#gid=656582295",sheet_name="ContainerCleaning")


    pti_log_df = scan_google_sheet(url="https://docs.google.com/spreadsheets/d/1L9qkq9WlIa2j5DcvoLvxkqYogRg76S-e8OxAIyLruAE/edit?gid=656582295#gid=656582295",sheet_name="ContainerPTI")
    return pti_log_df, washing_log_df


@app.cell
def _(pti_df, washing_df):
    summary_df = mo.sql(
        f"""
        WITH

            pti_main AS (
                FROM
                    pti_df
                SELECT
                    COALESCE(SUM(days_on_electricity), 0) AS QTY,
                    'PTI IOT SOC' AS Description,
                    'STD' AS Variant
            ), pti_plugin AS (
                FROM
                    pti_df
                SELECT
                    COUNT(plugin_price) AS QTY,
                    'PLUGIN' AS Description,
                    'STD' AS Variant
            ), pti_shifting AS (
                FROM
                    pti_df
                SELECT
                    COUNT(shifting_price) AS QTY,
                    'SHIFTING' AS Description,
                    'STD' AS Variant
               ),wash AS (
            FROM washing_df
            SELECT COUNT(*) AS QTY,
            'WASHING' AS Description,
            'STD' AS Variant
               )




        FROM pti_main
        UNION ALL
        FROM pti_plugin
        UNION ALL
        FROM pti_shifting
        UNION ALL
        FROM wash
        """,
        output=False
    )
    return (summary_df,)


@app.cell(hide_code=True)
def _(summary_df):
    pricing_df = mo.sql(
        f"""
        WITH bc AS (FROM
            price_df
        WHERE
            Description IN (
            'WASHING',
            'PLUGIN',
            'SHIFTING',
            'PTI IOT SOC'

            ) AND Variant = 'STD'),summaries AS (FROM summary_df)


        SELECT 
            b.Type,
            b."No.",
            b.Description,
            b.Variant,
            s.QTY::DECIMAL AS QTY,
            b."Unit Price"::DECIMAL AS "Unit Price"
        FROM bc b LEFT JOIN summaries s ON s.Description = b.Description AND s.Variant = b.Variant
        """
    )
    return (pricing_df,)


@app.cell
def _():
    copy_button = mo.ui.run_button(label="📋 Copy BC data to clipboard")
    save_button = mo.ui.run_button(label="💾 Save XLSX report")
    return copy_button, save_button


@app.cell
def _(bc_df, copy_button):
    mo.stop(not copy_button.value)
    bc_df.write_clipboard()  # tab-separated, pastes cleanly into BC / Excel
    mo.md("✅ Copied to clipboard")
    return


@app.cell(hide_code=True)
def _(pricing_df):
    bc_df = mo.sql(
        f"""
        FROM pricing_df
            SELECT * EXCLUDE("Unit Price")
        WHERE QTY > 0
        """
    )
    return (bc_df,)


@app.cell(hide_code=True)
def _(pricing_df):
    _df = mo.sql(
        f"""
        FROM pricing_df
            SELECT *, ("Unit Price" * QTY)::DECIMAL AS total_price
        WHERE QTY > 0
        """
    )
    return


@app.cell(hide_code=True)
def _(pricing_df):
    service_breakdown_df = mo.sql(
        f"""
        WITH
            mapped AS (
                FROM
                    pricing_df
                SELECT
                    CASE
                        WHEN Description = 'STUFFING - FISHLOADER' THEN 'Vessel Unloading Operations'
                    END AS section,
                    CASE
                        WHEN Description = 'STUFFING - FISHLOADER'
                        AND Variant = 'STD' THEN 'Normal Hours'
                        WHEN Description = 'STUFFING - FISHLOADER'
                        AND Variant = '150%' THEN 'Overtime 150%'
                        WHEN Description = 'STUFFING - FISHLOADER'
                        AND Variant = '200%' THEN 'Overtime 200%'
                    END AS service,
                    CASE
                        WHEN Description = 'STUFFING - FISHLOADER'
                        AND Variant = 'STD' THEN 1
                        WHEN Description = 'STUFFING - FISHLOADER'
                        AND Variant = '150%' THEN 2
                        WHEN Description = 'STUFFING - FISHLOADER'
                        AND Variant = '200%' THEN 3
                    END AS sort_order,
                    Variant,
                    COALESCE(QTY, 0)::DECIMAL AS QTY,
                    "Unit Price",
                    (COALESCE(QTY, 0) * "Unit Price")::DECIMAL AS line_total
            )
        FROM
            mapped
        SELECT
            section,
            service,
            sort_order,
            -- base rate shown on the invoice; OT variants only affect the total
            "Unit Price" AS unit_price,
            SUM(QTY) AS quantity,
            ROUND(SUM(line_total), 2) AS total
        GROUP BY
            section,
            service,
            sort_order,
            unit_price
            HAVING section IS NOT NULL
        ORDER BY
            sort_order
        """
    )
    return (service_breakdown_df,)


@app.cell
def _(
    days_on_electricity,
    month_selector,
    no_of_plugin,
    no_of_shifting,
    no_of_washing,
    pti_df,
    select_report,
    service_breakdown_df,
    washing_df,
):
    reports = {
        "Summary": {
            "type": "summary",
            "month": f"{format_datestr_to_month_year(month_selector.value)}",
            "client": f"{select_report.value}",
            "summary_rows": [
                None,  # blank row
                {
                    "label": "Container Cleaning:",
                    "value": no_of_washing,
                    "unit": "Containers",
                    "italic": True,
                },
                {
                    "label": "PTI:",
                    "value": None,
                    "unit": None,
                    "italic": True,
                },
                {
                    "label": "Shifting:",
                    "value": no_of_shifting,
                    "unit": "Shifts",
                    "italic": True,
                },
                {
                    "label": "Plugin",
                    "value": no_of_plugin,
                    "unit": "Plugins",
                    "italic": True,
                },
                None,
                {
                    "label": "Electricity",
                    "value": (days_on_electricity),
                    "unit": "Days",
                    # "number_format": "#,##0.000",
                },
            ],
            # Expected columns:
            # section, service, quantity, unit_price, total
            "service_df": service_breakdown_df,
            "landscape": False,
            "pages_wide": 1,
            "pages_tall": 1,
        },
        "Container Cleaning": {
            "df": washing_df,
            "header_color": "#f35f97",
        },"PTI Operation": {
            "df": pti_df,
            "header_color": "#f35f97",
        }
    }
    return (reports,)


@app.cell
def _(month_selector, reports, save_button, select_report):
    mo.stop(
        not save_button.value,
        mo.md("*Click the button to generate the file.*"),
    )

    output_file = export_dataframes(
        dataframes=reports,
        output_path=f"output/{select_report.value} CCCS - {format_datestr_to_month_year(month_selector.value)}.xlsx",
    )
    mo.md(f"✅ Saved to `{output_file}`")
    return


if __name__ == "__main__":
    app.run()
