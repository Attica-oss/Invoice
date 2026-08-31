"""Lazy re-exports for the type_casting package.

Importing ``type_casting`` costs nothing. Each name below is resolved on
first access -- including ``from type_casting import <name>`` -- via PEP
562 ``__getattr__``, which pulls it from the owning submodule (mirrors the
pattern in ``dataframe/__init__.py``).

Why lazy: ``containers`` and ``customers`` build their enums from a Google
Sheet at module import. Keeping them off the package import path means the
date / validation / enum helpers stay importable -- and unit-testable --
with no network. ``dates`` registers the ``days`` expression namespace on
load; every code path that uses ``.days.*`` imports a ``dates`` symbol
(directly or via this module), so it is always registered before use.
"""

from importlib import import_module
from typing import Any

# public name -> submodule that defines it
_SPEC: dict[str, str] = {
    # cast_to_numbers
    "CastToNumbers": "cast_to_numbers",
    "Numbers": "cast_to_numbers",
    # containers (network at import)
    "containers_enum": "containers",
    "iot_soc": "containers",
    "iot_soc_enum": "containers",
    # customers (network at import)
    "enum_customer": "customers",
    "shipper": "customers",
    "shipping_line": "customers",
    # dates (registers the `days` expression namespace on load)
    "CURRENT_YEAR": "dates",
    "LOWER_BOUND": "dates",
    "MIDNIGHT": "dates",
    "NULL_DURATION": "dates",
    "SPECIAL_DAYS": "dates",
    "UPPER_BOUND": "dates",
    "UPPER_BOUND_SPECIAL_DAY": "dates",
    "ZERO_DURATION": "dates",
    "DayName": "dates",
    "Days": "dates",
    "duration_to_hhmm": "dates",
    "public_holiday": "dates",
    # polars_enum
    "PolarsEnum": "polars_enum",
    # validations
    "MOVEMENT_TYPE": "validations",
    "PLUGGED_STATUS": "validations",
    "UNLOADING_SERVICE": "validations",
    "MovementType": "validations",
    "Overtime": "validations",
    "OvertimePerc": "validations",
    "PalletType": "validations",
    "SetPoint": "validations",
    "apply_overtime_rate": "validations",
}


def __getattr__(name: str) -> Any:
    submodule = _SPEC.get(name)
    if submodule is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return getattr(import_module(f"{__name__}.{submodule}"), name)


def __dir__() -> list[str]:
    return sorted(_SPEC)


__all__ = list(_SPEC)
