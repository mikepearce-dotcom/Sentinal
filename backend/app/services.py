"""Compatibility layer for the services module.

The full implementation lives in `app.services_impl`.
This file preserves the existing import path (`app.services`).
"""

from importlib import import_module

_impl = import_module(".services_impl", __package__)

for _name in dir(_impl):
    if _name.startswith("__"):
        continue
    globals()[_name] = getattr(_impl, _name)

del _impl
del _name
