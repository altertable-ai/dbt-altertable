from dbt.adapters.base import AdapterPlugin

from dbt.adapters.altertable.connections import AltertableCredentials
from dbt.adapters.altertable.impl import AltertableAdapter
from dbt.include import altertable

__all__ = ["AltertableAdapter", "AltertableCredentials", "Plugin"]

Plugin = AdapterPlugin(
    adapter=AltertableAdapter,
    credentials=AltertableCredentials,
    include_path=altertable.PACKAGE_PATH,
)
