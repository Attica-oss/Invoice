import marimo

__generated_with = "0.16.0"
app = marimo.App(width="medium")


@app.cell
def _():
    from dataframe.miscellaneous import by_catch,bycatch
    return (bycatch,)


@app.cell
def _():
    from type_casting.customers import enum_customer
    return (enum_customer,)


@app.cell
def _(enum_customer):
    enum_customer().categories
    return


@app.cell
def _(bycatch):
    bycatch
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
