"""Stores all dataframes as list and dicts"""

import polars as pl

from dataframe import (
    bin_dispatch,
    emr,
    invoice,
    miscellaneous,
    netlist,
    operations,
    shore_handling,
    stuffing,
    transport,
    washing_lf,
)

# Invoice Dataframes

invoice_dataframes: dict[str, pl.LazyFrame] = {
    "sto_invoice_status": invoice.sto_invoice_status(),
    "full_oss_invoice_status": invoice.full_oss_invoice_status(),
    "basic_oss_invoice_status": invoice.basic_oss_invoice_status(),
    "all_invoice_status": invoice.clean_all_invoice_status(),
    "cccs_oss_invoice_status": invoice.cccs_oss_invoice_status(),
}


# EMR Dataframes

emr_dataframes: dict[str, pl.LazyFrame] = {
    "shifting": emr.shifting,
    "washing": emr.washing,
    "pti": emr.pti,
}

# Washing DataFrame
washing_dataframe: dict[str, pl.LazyFrame] = {"washing": washing_lf}


# Miscellaneous Daframes

bin_dispatch_dataframes: dict[str, pl.LazyFrame] = {
    "full_scows_transfer": bin_dispatch.full_scows,
    "empty_scows_transfer": bin_dispatch.empty_scows,
}

miscellaneous_dataframes: dict[str, pl.LazyFrame] = {
    "static_loader": miscellaneous.static_loader,
    "dispatch_to_cargo": miscellaneous.dispatch_to_cargo,
    "truck_to_cccs": miscellaneous.truck_to_cccs,
    "cross_stuffing": miscellaneous.cross_stuffing,
    "cccs_stuffing": miscellaneous.cccs_stuffing,
    "bycatch": miscellaneous.by_catch,
    "from_cccs_to_vessel": miscellaneous.from_cccs_to_vessel,
    "truck_to_cccs_via_skiff": miscellaneous.truck_to_cccs_via_skiff,
}

netlist_dataframes: dict[str, pl.LazyFrame] = {
    "net_list": netlist.netList,
    "iot_container_stuffing": netlist.iot_stuffing,
    "oss_stuffing": netlist.oss,
    "iot_cargo_discharge": netlist.iot_cargo,
}

operations_dataframes: dict[str, pl.LazyFrame] = {
    "ops": operations.ops,
    "extramen": operations.extramen,
    "hatch_to_hatch": operations.hatch_to_hatch,
    "additional_overtime": operations.additional,
    "tare_calibration": operations.tare,
    "berth_dues": operations.berth,
}

# Names backed (directly or transitively) by dataframe.operations's Excel
# readers, which data_source.make_dataset.is_windows() gates to Windows --
# elsewhere these are empty, zero-column frames rather than a real failure.
EXCEL_BACKED_NAMES: frozenset[str] = frozenset(
    {"extramen", "hatch_to_hatch", "additional_overtime", "berth_dues"}
)

shore_handling_dataframes: dict[str, pl.LazyFrame] = {
    "salt": shore_handling.salt,
    # "bin_tipping": shore_handling.bin_tipping,
    "forklift_salt": shore_handling.forklift_salt(),
}

stuffing_dataframes: dict[str, pl.LazyFrame] = {
    "pallet_liner": stuffing.pallet,
    "container_plugin": stuffing.coa,
}

transport_dataframes: dict[str, pl.LazyFrame] = {
    "shore_crane": transport.shore_crane,
    "transfer": transport.transfer,
    "scow_transfer": transport.scow_transfer,
    "forklift": transport.forklift,
}
