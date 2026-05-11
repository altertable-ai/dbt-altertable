import contextlib

from dbt.adapters.base import AdapterPlugin

from dbt.adapters.altertable.credentials import AltertableCredentials
from dbt.adapters.altertable.impl import AltertableAdapter
from dbt.include import altertable

with contextlib.suppress(ModuleNotFoundError):
    from dbt.adapters.altertable import metricflow as _  # noqa: F401

__all__ = ["AltertableAdapter", "AltertableCredentials", "Plugin"]

Plugin = AdapterPlugin(
    adapter=AltertableAdapter,
    credentials=AltertableCredentials,
    include_path=altertable.PACKAGE_PATH,
)
