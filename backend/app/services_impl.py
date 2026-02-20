"""Aggregated services implementation.

This module re-exports symbols from split service modules while preserving
legacy attribute access (including underscore-prefixed helpers).
"""

from importlib import import_module

_module_names = (
    ".services_common",
    ".services_fetch",
    ".services_analysis",
    ".services_breakdown",
)

for _module_name in _module_names:
    _mod = import_module(_module_name, __package__)
    for _name in dir(_mod):
        if _name.startswith("__"):
            continue
        globals()[_name] = getattr(_mod, _name)

del _mod
del _module_name
del _module_names
del _name
