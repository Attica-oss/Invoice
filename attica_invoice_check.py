import marimo

__generated_with = "0.16.0"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells
    import polars as pl

    import marimo as mo


@app.cell
def _():
    from dataframe.emr import shifting
    return (shifting,)


@app.cell
def _(shifting):
    _df = mo.sql(
        f"""
        SELECT * FROM shifting;
        """
    )
    return


@app.cell
def _():
    shifting_path = r"C:\Users\gmounac\Dropbox\Container and Transport\Transport Section\Container Shifting Records\IPHS Container Shifting Record.xlsx"
    return (shifting_path,)


@app.cell
def _(shifting_path):
    shifting_log = pl.read_excel(shifting_path)
    return (shifting_log,)


@app.cell
def _(shifting_log):
    _df = mo.sql(
        f"""
        SELECT
            *
        FROM
            shifting_log
        WHERE
            YEAR("Date Shifted") = 2025
            AND MONTH("Date Shifted") = 9;
        """
    )
    return


@app.cell
def _():
    transfer_path = r"C:\Users\gmounac\Dropbox\Container and Transport\Transport Section\Container Movements\Container Transfer.xlsx"

    transfer_df = pl.read_excel(transfer_path, sheet_name="Transfer")
    return (transfer_df,)


@app.cell
def _(transfer_df):
    raw = mo.sql(
        f"""
        SELECT
            *
        FROM
            transfer_df
        WHERE
            Status = 'Empty'
            AND YEAR(Date) = 2025
            AND Month(Date) = 9 AND Remarks = 'Maersk Depot';
        """
    )
    return (raw,)


@app.cell
def _():
    maersk_path = r"A:\INVOICING!!\2025\ShippingLines\MAERSK\09 - September\Maersk Depot\Maersk Monthly Trucking - September '25.xlsx"

    maersk = (
        pl.read_excel(maersk_path, sheet_name="Haulage", has_header=False)
        .select(pl.col("column_4"), pl.col("column_2"))
        .filter(
            pl.col("column_4").is_not_null().and_(pl.col("column_4").ne("Date"))
        )
        .rename({"column_4": "date", "column_2": "container_number"})
        .select(
            pl.col("date")
            .str.replace(" 00:00:00", "")
            .str.to_date(format="%Y-%m-%d"),
            pl.col("container_number"),
        )
    )
    return (maersk,)


@app.cell
def _(maersk, raw):
    raw.join(
        maersk,
        left_on=["Date", "Container Ref. No."],
        right_on=["date", "container_number"],
        how="full",
    )
    return


@app.cell
def _():
    forklift_path = r"C:\Users\gmounac\Dropbox\Container and Transport\Transport Section\Forklift Usage\Forklift Record.xlsx"
    return (forklift_path,)


@app.cell
def _(forklift_path):
    forklift_df = pl.read_excel(forklift_path)
    return (forklift_df,)


@app.cell
def _(forklift_df):
    _df = mo.sql(
        f"""
        SELECT
            "Date of Service" as date,
            Driver as driver,
            CAST("Time Out" as time) as time_out,
            CAST("Time In" as time) as time_in,
            UPPER("Vessel/Client") as customer,
            "Purpose" as purpose,
            "Invoiced in:" as invoiced_in
        FROM
            forklift_df
        WHERE
            YEAR(date) = 2025
            AND MONTH(date) = 9
            AND (
                purpose LIKE 'Salt loading'
                OR purpose LIKE 'salt loading'
            )
        ORDER BY
            date,
            driver,
            time_in ASC;
        """
    )
    return


@app.cell
def _():
    from utils.google_sheet import GoogleSheetsLoader
    return (GoogleSheetsLoader,)


@app.cell
def _(GoogleSheetsLoader):
    gs = GoogleSheetsLoader()
    return (gs,)


@app.cell
def _(gs):
    washing_log = gs.load_sheet(config_name="container_cleaning_log").data
    return (washing_log,)


@app.cell
def _(washing_log):
    washing_log.columns
    return


@app.cell
def _(washing_log):
    _df = mo.sql(
        f"""
        SELECT
            date,
            container_number,
            shipping_line,
            cleaning_remarks,
            "Invoice To" as invoice_to,

        FROM
            washing_log 
        WHERE container_number = 'SUDU8041577';
        """
    )
    return


@app.cell
def _(gs):
    plugin_log = gs.load_sheet(config_name="container_plugin_log").data
    plugin_inv = gs.load_sheet(config_name="electricity").data
    return plugin_inv, plugin_log


@app.cell
def _():
    from dataframe.transport import transfer
    return (transfer,)


@app.cell
def _(transfer):
    _df = mo.sql(
        f"""
        SELECT * FROM transfer;
        """
    )
    return


@app.cell
def _():
    from datetime import datetime
    return (datetime,)


@app.cell
def _(datetime):
    datetime.now().strftime(format="%d/%m/%Y %H:%M:%S")
    return


@app.cell
def _(plugin_log):
    log = plugin_log.filter(pl.col("Location").eq("On Plug")).collect()

    log
    return (log,)


@app.cell
def _(log, plugin_inv):
    log.join(
        plugin_inv.collect(),
        left_on=["Date", "Container Number"],
        right_on=["date_plugged", "container_number"],
        how="left",
    ).filter(pl.col("customer").is_not_null()).select(pl.col("Date"),pl.col("Container Number"),pl.col("date_out"),pl.col("location"))
    return


@app.cell
def _(plugin_inv):
    _df = mo.sql(
        f"""
        SELECT * FROM plugin_inv;
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
