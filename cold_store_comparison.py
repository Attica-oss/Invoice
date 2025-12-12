import marimo

__generated_with = "0.16.0"
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
    tipping_truck = gs.load_sheet(config_name="Tipping_Truck").data
    return (tipping_truck,)


@app.cell
def _(gs):
    netList = gs.load_sheet(config_name="Operations_Activity").data.pipe(
        add_overtime_col
    )
    return


@app.function
def add_overtime_col(dataf: pl.LazyFrame):
    return dataf.with_columns(
        overtime=pl.when(
            (
                pl.col("Day")
                .is_in(["PH", "Sun"])
                .and_(pl.col("overtime").eq(pl.lit("overtime 150%")))
            ).or_(
                pl.col("Day")
                .is_in(["PH", "Sun"])
                .not_()
                .and_(pl.col("overtime").eq(pl.lit("normal hours")))
            )
        )
        .then(pl.lit("normal"))
        .otherwise(pl.lit("overtime"))
    )


@app.cell
def _(netlist):
    genesis_dataf = mo.sql(
        f"""
        SELECT
            day,
            date,
            UPPER(vessel) as vessel,
            destination,
            SUM(COALESCE(ROUND("normal", 3), 0)) as normal_hours,
            SUM(COALESCE(ROUND("overtime", 3), 0)) as overtime_hours,
            (normal_hours + overtime_hours) as total_tonnage,
            -- storage_type
        FROM
            (
                SELECT
                    day,
                    date,
                    vessel,
                    "Container (Destination)" as destination,
                    overtime,
                    -- "Storage" as storage_type,
                    CAST(
                        REPLACE("Scale Reading(-Fish Net) (Cal)", ',', '') as INT
                    ) * 0.001 as total_tonnage
                FROM
                    netList
                WHERE
                    destination LIKE 'CCCS%'
            )
        PIVOT (
            SUM(total_tonnage) FOR overtime IN ('normal', 'overtime')
        )
        GROUP BY 
            day,
            date,
            vessel,
            destination

        ORDER BY
            Date;
        """
    )
    return (genesis_dataf,)


@app.cell
def _(tipping_truck):
    cccs_dataf = mo.sql(
        f"""
        SELECT * FROM tipping_truck WHERE operation_type = 'To CCCS via Truck';
        """
    )
    return (cccs_dataf,)


@app.cell
def _(cccs_dataf, genesis_dataf):
    _df = mo.sql(
        f"""
        SELECT
            genesis_dataf.date,
            genesis_dataf.vessel,
            genesis_dataf.total_tonnage as genesis_tonnage,
            cccs_dataf.total_tonnage as cccs_tonnage,
            ROUND(cccs_tonnage - genesis_tonnage,3) as diff
        FROM
            genesis_dataf
            LEFT JOIN cccs_dataf ON (genesis_dataf.date = cccs_dataf.date)
            AND (genesis_dataf.vessel = cccs_dataf.vessel);
        """
    )
    return


@app.cell
def _():
    208.427
    return


if __name__ == "__main__":
    app.run()
