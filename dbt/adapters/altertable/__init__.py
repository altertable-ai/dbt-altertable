from dbt.adapters.altertable.connections import AltertableCredentials
from dbt.adapters.altertable.impl import AltertableAdapter
from dbt.adapters.base import AdapterPlugin
from dbt.include import altertable

Plugin = AdapterPlugin(
    adapter=AltertableAdapter,
    credentials=AltertableCredentials,
    include_path=altertable.PACKAGE_PATH,
)
