import marimo

__generated_with = "0.19.6"
app = marimo.App()


@app.cell
def _():
    import re
    import duckdb
    from datetime import datetime
    import marimo as mo
    return datetime, duckdb, mo, re


@app.cell
def _(duckdb):
    # Use a file path if you want persistence, e.g. "cleaning_log.duckdb"
    con = duckdb.connect(database=":memory:")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS cleaning_log (
            cleaning_date DATE,
            container_number VARCHAR,
            shipping_line VARCHAR,
            cleaning_remarks VARCHAR,
            comments VARCHAR,
            created_at TIMESTAMP
        );
        """
    )
    return (con,)


@app.cell
def _(re):
    CONTAINER_FORMAT = re.compile(r"^[A-Z]{4}\d{7}$")
    VALID_FOURTH = {"U", "J", "Z"}

    ISO6346_MAP = {
        **{str(i): i for i in range(10)},
        "A": 10, "B": 12, "C": 13, "D": 14, "E": 15, "F": 16, "G": 17, "H": 18,
        "I": 19, "J": 20, "K": 21, "L": 23, "M": 24, "N": 25, "O": 26, "P": 27,
        "Q": 28, "R": 29, "S": 30, "T": 31, "U": 32, "V": 34, "W": 35, "X": 36,
        "Y": 37, "Z": 38,
    }

    def iso6346_check_digit(first10: str) -> int | None:
        if len(first10) != 10:
            return None
        total = 0
        for i, ch in enumerate(first10):
            if ch not in ISO6346_MAP:
                return None
            total += ISO6346_MAP[ch] * (2**i)
        remainder = total % 11
        return 0 if remainder == 10 else remainder

    def validate_container(container: str) -> tuple[bool, str]:
        c = (container or "").strip().upper()
        if not c:
            return False, "Container number is required."

        if not CONTAINER_FORMAT.fullmatch(c):
            return False, "Invalid format. Must be 4 letters + 7 digits (e.g., AMCU9350194)."

        if c[3] not in VALID_FOURTH:
            return False, "Fourth letter must be U, J, or Z."

        expected = iso6346_check_digit(c[:10])
        if expected is None:
            return False, "Container contains invalid characters."
        actual = int(c[-1])
        if expected != actual:
            return False, f"Invalid check digit. Expected {expected}, got {actual}."

        return True, ""
    return (validate_container,)


@app.cell
def _():
    # css = mo.Html(r"""
    # <style>
    # /* --- your CSS (unchanged) --- */
    # * { margin: 0; padding: 0; box-sizing: border-box; }

    # body {
    #   font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
    #   line-height: 1.6;
    #   margin: 0;
    #   padding: 20px;
    #   background-color: #f5f5f5;
    # }

    # .header {
    #   text-align: center;
    #   padding: 2rem 1rem;
    #   background-color: #fff;
    #   border-radius: 8px;
    #   margin-bottom: 2rem;
    #   box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    # }

    # .header h1 { font-size: 1.8rem; margin-bottom: 0.5rem; color: #2c3e50; }
    # .header p { color: #666; margin: 0; }

    # .container {
    #   margin: 0 auto;
    #   background-color: white;
    #   border-radius: 8px;
    #   box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    #   max-width: 1200px;
    #   padding: 1rem;
    # }

    # h2, .header h1 { text-align: center; color: #333; margin-bottom: 30px; }

    # .form-group { margin-bottom: 15px; }

    # label { display: block; margin-bottom: 5px; font-weight: bold; }

    # input[type="date"],
    # input[type="text"],
    # input[type="time"],
    # input[type="number"],
    # select {
    #   width: 100%;
    #   padding: 8px;
    #   border: 1px solid #ddd;
    #   border-radius: 4px;
    #   box-sizing: border-box;
    #   font-size: 16px;
    # }

    # button,.btn {
    #   background-color: #4CAF50;
    #   color: white;
    #   padding: 10px 20px;
    #   border: none;
    #   border-radius: 4px;
    #   cursor: pointer;
    #   font-size: 16px;
    #   font-weight: bold;
    #   text-decoration: none;
    #   transition: background-color 0.2s;
    # }

    # textarea {
    #   width: 100%;
    #   padding: 8px;
    #   border: 1px solid #ddd;
    #   border-radius: 4px;
    #   box-sizing: border-box;
    #   font-size: 16px;
    #   font-family: Arial, sans-serif;
    #   resize: vertical;
    # }

    # button:disabled { background-color: #cccccc; cursor: not-allowed; }
    # button:hover, .btn:hover { background-color: #45a049; }

    # .alert {
    #   padding: 10px;
    #   margin-bottom: 15px;
    #   border-radius: 4px;
    #   display: none;
    # }
    # .alert-success { background-color: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
    # .alert-error { background-color: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }

    # .error { color: #dc3545; font-size: 14px; margin-top: 5px; display: none; }

    # .back-link { background-color: transparent; border: none; color: #666; }
    # .back-link:hover { text-decoration: underline; color: #666; }

    # .form-row { display: flex; gap: 1rem; margin-bottom: 1rem; }
    # .form-col { flex: 1; }

    # .required-field::after { content: " *"; color: #dc3545; }

    # /* Responsive */
    # @media (max-width: 600px) {
    #   .forms-grid { grid-template-columns: 1fr; }
    # }

    # /* --- marimo overrides (important) --- */
    # /* marimo alerts must be visible when rendered */
    # .alert { display: block; }

    # /* optional: make marimo widgets fill nicely */
    # [data-marimo-widget] { width: 100%; }
    # </style>
    # """)

    # today = datetime.now().date()

    return


@app.cell
def _(datetime, mo):
    css = mo.md(
        """
        <style>
          .container { max-width: 760px; margin: 0 auto; padding: 18px; }
          .title { font-size: 1.4rem; font-weight: 700; margin-bottom: 12px; }
          .alert { display: block; padding: 10px 12px; border-radius: 10px; margin: 10px 0 16px; }
          .alert-success { background: #e9f7ef; border: 1px solid #b7e2c4; }
          .alert-error { background: #fdecea; border: 1px solid #f5c2c7; }
          .hint { font-size: 0.85rem; opacity: 0.75; margin: 6px 0 14px; }
        </style>
        """
    )
    today = datetime.now().date()
    return css, today


@app.cell
def _(mo, today):
    # Inputs (no form wrapper)
    cleaning_date = mo.ui.date(value=today, label="Cleaning Date*")

    container_number = mo.ui.text(
        label="Container Number*",
        placeholder="e.g., AMCU9350194",
    )

    shipping_line = mo.ui.dropdown(
        label="Shipping Line*",
        options=["", "CMA CGM", "IOT", "MAERSK"],
        value="",
    )

    cleaning_remarks = mo.ui.dropdown(
        label="Visual Inspection/Remarks*",
        options=["", "Clean", "Rewash", "Unclean", "Other"],
        value="",
    )

    comments = mo.ui.text_area(
        label="Comments",
        placeholder="Leave comments if any...",
    )

    submit = mo.ui.run_button(label="Submit")
    return (
        cleaning_date,
        cleaning_remarks,
        comments,
        container_number,
        shipping_line,
        submit,
    )


@app.cell
def _(
    cleaning_date,
    cleaning_remarks,
    comments,
    con,
    container_number,
    mo,
    shipping_line,
    submit,
    validate_container,
):


    alert = None

    # In older marimo, button.value toggles/updates when clicked
    if submit.value:
        dt = cleaning_date.value
        ctn = (container_number.value or "").strip().upper()
        ship = shipping_line.value or ""
        rem = cleaning_remarks.value or ""
        com = comments.value or ""

        if dt is None:
            alert = mo.Html('<div class="alert alert-error">Cleaning date is required.</div>')
        else:
            ok, msg = validate_container(ctn)
            if not ok:
                alert = mo.Html(f'<div class="alert alert-error">{msg}</div>')
            elif not ship:
                alert = mo.Html('<div class="alert alert-error">Shipping line is required.</div>')
            elif not rem:
                alert = mo.Html('<div class="alert alert-error">Remarks are required.</div>')
            else:
                con.execute(
                    "INSERT INTO cleaning_log VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
                    [dt, ctn, ship, rem, com],
                )
                alert = mo.Html('<div class="alert alert-success">Saved cleaning log entry.</div>')

                # Optional: clear fields except date
                container_number.value = ""
                shipping_line.value = ""
                cleaning_remarks.value = ""
                comments.value = ""

    latest = con.execute(
        """
        SELECT *
        FROM cleaning_log
        ORDER BY created_at DESC
        LIMIT 50
        """
    ).df()

    mo.stop(not submit.value)
    return alert, latest


@app.cell
def _(
    alert,
    cleaning_date,
    cleaning_remarks,
    comments,
    container_number,
    css,
    latest,
    mo,
    shipping_line,
    submit,
):
    header = mo.Html(
        '<div class="container">'
        '<div class="title">IPHS Container Cleaning Log Sheet</div>'
        '<div class="hint">Format: 4 letters + 7 digits; 4th letter U/J/Z; ISO6346 check digit enforced.</div>'
        '</div>'
    )

    mo.vstack(
        [
            css,
            header,
            alert if alert is not None else mo.Html(""),
            cleaning_date,
            container_number,
            shipping_line,
            cleaning_remarks,
            comments,
            submit,
            mo.md("### Latest submissions"),
            mo.ui.table(latest),
        ]
    )
    return


if __name__ == "__main__":
    app.run()
