import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")

with app.setup:
    import polars as pl
    import marimo as mo
    # # from dataframe.operations import tare
    # from data_source.make_dataset import load_gsheet_data
    # from data_source.make_dataset import ExcelFiles,load_excel
    # from data_source.sheet_ids import OPS_SHEET_ID, raw_sheet
    from dataframe.netlist import netList
    # from data.price import get_price

    from read_google_sheet import read_google_sheet


@app.cell
def _():
    from data_source.make_dataset import load_gsheet_data

    return (load_gsheet_data,)


@app.cell
def _():
    read_google_sheet(sheet_id="1L9qkq9WlIa2j5DcvoLvxkqYogRg76S-e8OxAIyLruAE",sheet_name="ContainerGateOut")
    return


@app.cell
def _(load_gsheet_data):
    load_gsheet_data(sheet_id="1L9qkq9WlIa2j5DcvoLvxkqYogRg76S-e8OxAIyLruAE",sheet_name="ContainerGateOut")
    return


@app.cell
def _():
    from type_casting.containers import container_list,containers_enum

    return (containers_enum,)


@app.cell
def _(containers_enum):
    containers_enum
    return


@app.cell
def _(ExcelFiles, load_excel):
    berth_df = load_excel(file_path=ExcelFiles.BERTH_DUES_2026).select(
        pl.col("VESSEL NAME").alias("vessel"),
        pl.col("DATE IN").dt.combine(pl.col("TIME IN").dt.time()).alias("date_in"),
        pl.col("DATE OUT").dt.combine(pl.col("TIME OUT").dt.time()).alias("date_out"),
        pl.col("DURATION IN PORT").alias("day(s)_in_port"),
        pl.col("DISCOUNT").fill_null(0).alias("discount"),
        pl.col("INVOICE VALUE").alias("invoice_value"),
        pl.col("INVOICING ENTITY").alias("customer"),
        pl.col("VESSEL TYPE").alias("vessel_type"),
        pl.col("COMMENTS")
    )
    return (berth_df,)


@app.cell
def _(berth_df, netlist):
    berthing_df = mo.sql(
        f"""
        WITH
            berth AS (
                SELECT
                    *
                FROM
                    berth_df
            ),
            unloading AS (
                SELECT
                    date,
                    vessel,
            		CAST(SUM(total_tonnage) AS DECIMAL) AS tonnage
                FROM
                    netList
                WHERE
                    YEAR(date) = 2026
                GROUP BY
                    date,
                    vessel
            ),
            berth_fees_march AS (
                SELECT
                    *
                FROM
                    (
                        VALUES
                            ('FISHING VESSEL', 5000.00, 1500.00),
                            ('CARGO REEFER', 1000.00, 300.00),
                            ('SUPPLY BOAT', 800.00, 200.00),
                            ('MILITARY VESSEL', 12000.00, 3000.00),
                            ('LONGLINER', 800.00, 300.00)
                    ) AS t (vessel_type, first_4_day, any_additional_24h)
            ),
            discount_ AS (
                SELECT
                    *
                FROM
                    (
                        VALUES
                            ('ECHEBASTAR FLEET SLU', 1250.00),
                            ('HARTSWATER LIMITED', 1250.00)
                    ) AS t (client, price)
            ),
            add_comments AS (
                SELECT
                    b.*,
            		SUM(COALESCE(u.tonnage,0)) AS tonnage,
                    -- unloading days + comment
                    COUNT(u.date) AS unloading_days,
                    CASE
                        WHEN COUNT(u.date) = 0 THEN ''
                        WHEN COUNT(u.date) = 1 THEN '1 day unloading'
                        ELSE CAST(COUNT(u.date) AS VARCHAR) || ' days unloading'
                    END AS comments,
                    -- duration: replace this with your existing field if you have one
                    -- DATE_DIFF('day', CAST(b.date_in AS DATE), CAST(b.date_out AS DATE)) + 1 AS duration_in_port,
                    CASE
                        WHEN b.date_out IS NULL THEN NULL
                        ELSE DATE_DIFF(
                            'day',
                            CAST(b.date_in AS DATE),
                            CAST(b.date_out AS DATE)
                        ) + CASE
                            WHEN CAST(b.date_out AS TIME) < CAST(b.date_in AS TIME) THEN 0
                            ELSE 1
                        END
                    END AS duration_in_port,
                    -- price logic (Excel translation)
                    CASE
                        WHEN b.date_out IS NULL THEN 0
                        WHEN b.customer = 'PRINCESS TUNA' THEN 0
                        WHEN b.customer IN ('ECHEBASTAR FLEET SLU', 'HARTSWATER LIMITED')
                        AND b.vessel_type = 'FISHING VESSEL' THEN COALESCE(d.price, 0) * duration_in_port
                        WHEN duration_in_port = 4 THEN COALESCE(f.first_4_day, 0)
                        WHEN duration_in_port < 4 THEN COALESCE(f.any_additional_24h, 0) * duration_in_port
                        WHEN duration_in_port > 4 THEN (duration_in_port - 4) * COALESCE(f.any_additional_24h, 0) + COALESCE(f.first_4_day, 0)
                        ELSE 0
                    END AS berth_fee
                FROM
                    berth b
                    LEFT JOIN unloading u ON u.date BETWEEN CAST(b.date_in AS DATE) AND CAST(b.date_out AS DATE)
                    AND u.vessel = b.vessel
                    LEFT JOIN berth_fees_march f ON f.vessel_type = b.vessel_type
                    LEFT JOIN discount_ d ON d.client = b.customer
                GROUP BY ALL
                ORDER BY
                    b.date_in
            )
        FROM
            add_comments
        SELECT
            vessel,
            customer,
            date_in,
            date_out,
            duration_in_port,

            discount,
            berth_fee,
            (berth_fee + discount) AS total_value,
          -- final comment: base comments + // + tonnage+t (only if tonnage > 0)
          comments_1
          || CASE
               WHEN COALESCE(tonnage, 0) = 0 THEN ''
               ELSE ' // ' || CAST(tonnage AS VARCHAR) || 't'
             END AS comments_final
        """
    )
    return


@app.cell
def _():
    1500 * 6
    return


@app.cell
def _():
    color_scheme = '#FFF2CC'
    return


@app.cell
def _(get_price):
    TARE_RATE: pl.LazyFrame = get_price(
        ["Rental of Calibration", "Tare Calibration"]
    ).select(pl.col("Date").alias("effective_date"),pl.col("Service").alias("service"),pl.col("Price").alias("unit_price"),)
    return (TARE_RATE,)


@app.cell
def _(OPS_SHEET_ID, load_gsheet_data, raw_sheet):
    test_tare = load_gsheet_data(OPS_SHEET_ID, raw_sheet).select(
                pl.col("Date").alias("date"),
                pl.col("Vessel").str.to_uppercase().alias("vessel"),
                pl.col("Side Working").alias("side_working"),
                pl.col("Container (Destination)")
            )
    return (test_tare,)


@app.cell(hide_code=True)
def _(test_tare):
    _df = mo.sql(
        f"""
        FROM test_tare WHERE Date BETWEEN '2026-01-01' AND '2026-01-06' AND Vessel = 'EGALABUR'
        """
    )
    return


@app.cell
def _(OPS_SHEET_ID, TARE_RATE: pl.LazyFrame, load_gsheet_data, raw_sheet):
    tare_dataf = (
        (
            (
                load_gsheet_data(OPS_SHEET_ID, raw_sheet)
                .select(
                    pl.col("Date").alias("date"),
                    pl.col("Vessel").str.to_uppercase().alias("vessel"),
                    pl.col("Side Working").alias("side_working"),
                )
                .unique()
                .sort(by="date")
                .group_by(["date", "vessel"], maintain_order=True)
                .agg(
                    pl.col("side_working")
                    .unique()
                    .sort()
                    .str.join(", ")
                    .alias("side_working")
                )
            )
            .with_columns(
                [
                    pl.lit(1, dtype=pl.Int64).alias("rental_of_weight"),
                    pl.col("side_working")
                    .str.split(", ")
                    .list.len()
                    .alias("number_of_sides"),
                    pl.lit("Rental of Calibration").alias("service"),
                ]
            )
            .join_asof(
                other=TARE_RATE,
                by_left="service",
                by_right="service",
                left_on="date",
                right_on="effective_date",
                strategy="backward",
            )
            .drop(["service","effective_date"])
        )
        .with_columns(
            [
                (pl.col("unit_price") * pl.col("rental_of_weight")).alias(
                    "price_per_rental"
                )
            ]
        )
        .drop(pl.col("unit_price"))
    ).with_columns(pl.lit("Tare Calibration").alias("service")).join_asof(
                other=TARE_RATE,
                by_left="service",
                by_right="service",
                left_on="date",
                right_on="effective_date",
                strategy="backward",
            ) .drop(["service","effective_date"]).with_columns(
            [
                (pl.col("unit_price") * pl.col("number_of_sides")).alias(
                    "price_per_calibrations"
                )
            ]
        ).drop(pl.col("unit_price")).with_columns([(pl.col("price_per_rental")+pl.col("price_per_calibrations")).alias("total_price")])
    return (tare_dataf,)


@app.cell
def _(tare_dataf):
    tare_dataf
    return


@app.cell(hide_code=True)
def _(tare_dataf):
    tare_df = mo.sql(
        f"""
        FROM tare_dataf WHERE Date BETWEEN '2026-01-01' AND '2026-01-06' AND Vessel = 'EGALABUR'
        """
    )
    return (tare_df,)


@app.cell
def _(tare_df):
    # Tare Weight

    tare_df.group_by(["number_of_sides","rental_of_weight"]).agg(pl.col("price_per_rental").sum(),pl.col("price_per_calibrations").sum())
    return


@app.cell
def _():
    from dataframe.transport import forklift
    from dataframe.shore_handling import forklift_salt

    return forklift, forklift_salt


@app.cell
def _(forklift_salt):
    f_salt = forklift_salt()
    return (f_salt,)


@app.cell(hide_code=True)
def _(f_salt):
    _df = mo.sql(
        f"""
        FROM f_salt WHERE vessel = 'EGALABUR' AND YEAR(date) = 2026
        """
    )
    return


@app.cell(hide_code=True)
def _(forklift):
    _df = mo.sql(
        f"""
        FROM forklift WHERE customer = 'EGALABUR' AND date > '2025-12-31'
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
