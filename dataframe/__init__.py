"""Lazy re-exports for the dataframe package.

Nothing is imported or built at package import time. Each name below is
resolved on first access (including ``from dataframe import <name>``) by
:pep:`562` ``__getattr__``, which pulls it from the owning submodule --
whose frames are themselves ``@lru_cache`` builders. Result: importing
``dataframe`` costs nothing; the first use of a frame triggers exactly the
sheet reads it needs, cached for the process.
"""

from importlib import import_module
from typing import Any

# name -> (submodule, attribute, call_result?)
#   call_result=True  -> the submodule attribute is a builder function and the
#                        re-export should be the *frame* it returns.
_SPEC: dict[str, tuple[str, str, bool]] = {
    # bin_dispatch
    "full_scows": ("bin_dispatch", "full_scows", False),
    "empty_scows": ("bin_dispatch", "empty_scows", False),
    # emr
    "pti": ("emr", "pti", False),
    "shifting": ("emr", "shifting", False),
    # washing / pti helpers (functions kept as functions, like before)
    "washing": ("washing", "washing", False),
    "washing_lf": ("washing", "washing", True),
    # miscellaneous
    "by_catch": ("miscellaneous", "by_catch", False),
    "cccs_stuffing": ("miscellaneous", "cccs_stuffing", False),
    "cross_stuffing": ("miscellaneous", "cross_stuffing", False),
    "dispatch_to_cargo": ("miscellaneous", "dispatch_to_cargo", False),
    "from_cccs_to_vessel": ("miscellaneous", "from_cccs_to_vessel", False),
    "truck_to_cccs_via_skiff": ("miscellaneous", "truck_to_cccs_via_skiff", False),
    # netlist
    "netList": ("netlist", "netList", False),
    "oss": ("netlist", "oss", False),
    "iot_cargo": ("netlist", "iot_cargo", False),
    "iot_stuffing": ("netlist", "iot_stuffing", False),
    # shore_handling
    "salt": ("shore_handling", "salt", False),
    "forklift_salt": ("shore_handling", "forklift_salt", False),
    "forklift_for_salt": ("shore_handling", "forklift_salt", True),
    # stuffing
    "coa": ("stuffing", "coa", False),
    "pallet": ("stuffing", "pallet", False),
    # transport
    "forklift": ("transport", "forklift", False),
    "scow_transfer": ("transport", "scow_transfer", False),
    "shore_crane": ("transport", "shore_crane", False),
    "transfer": ("transport", "transfer", False),
    # operations
    "berth": ("operations", "berth", False),
    "extramen": ("operations", "extramen", False),
    "additional": ("operations", "additional", False),
    "hatch_to_hatch": ("operations", "hatch_to_hatch", False),
}


def __getattr__(name: str) -> Any:
    spec = _SPEC.get(name)
    if spec is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    submodule, attribute, call_result = spec
    value = getattr(import_module(f"{__name__}.{submodule}"), attribute)
    return value() if call_result else value


def __dir__() -> list[str]:
    return sorted(_SPEC)


__all__ = list(_SPEC)
