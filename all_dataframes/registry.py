"""Registry for all datasets builders. This allows for easy access to all datasets and their builders in a structured way."""

from __future__ import annotations

from collections.abc import Callable
import polars as pl
from polars_result import Result

from dataframe import (
    emr,
    operations,
    netlist,
    bin_dispatch,
    shore_handling,
    stuffing,
    transport,
    miscellaneous,
)

DatasetBuilder = Callable[[], Result[pl.LazyFrame, Exception]]
DatasetCategory = dict[str, DatasetBuilder]
DatasetRegistry = dict[str, DatasetCategory]

REGISTRY: DatasetRegistry = {
    "emr": {
        "shifting": emr.build_shifting,
        "pti": emr.build_pti,
        "washing": emr.build_washing,
    },
    "operations": {
        "ops": operations.build_ops,
        "tare_calibration": operations.build_tare,
    },
    "netlist": {
        "net_list": netlist.build_net_list,
        "iot_container_stuffing": netlist.build_iot_stuffing,
        "oss_stuffing": netlist.build_oss,
        "iot_cargo_discharge": netlist.build_iot_cargo,
    },
    "bin_dispatch": {
        "full_scows_transfer": bin_dispatch.build_full_scows,
        "empty_scows_transfer": bin_dispatch.build_empty_scows,
    },
    "shore_handling": {
        "salt": shore_handling.build_salt,
        "forklift_salt": shore_handling.build_forklift_salt,
    },
    "stuffing": {
        "pallet_liner": stuffing.build_pallet,
        "container_plugin": stuffing.build_coa,
    },
    "transport": {
        "shore_crane": transport.build_shore_crane,
        "transfer": transport.build_transfer,
        "scow_transfer": transport.build_scow_transfer,
        "forklift": transport.build_forklift,
    },
    "miscellaneous": {
        "static_loader": miscellaneous.build_static_loader,
        "dispatch_to_cargo": miscellaneous.build_dispatch_to_cargo,
        "truck_to_cccs": miscellaneous.build_truck_to_cccs,
        "cross_stuffing": miscellaneous.build_cross_stuffing,
        "cccs_stuffing": miscellaneous.build_cccs_stuffing,
        "bycatch": miscellaneous.build_by_catch,
        "from_cccs_to_vessel": miscellaneous.build_from_cccs_to_vessel,
    },
}
