from dbt.adapters.sql import SQLAdapter

from dbt.adapters.altertable.connections import AltertableConnectionManager


class AltertableAdapter(SQLAdapter):
    ConnectionManager = AltertableConnectionManager

    @classmethod
    def date_function(cls) -> str:
        return "now()"
