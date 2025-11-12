from dbt.adapters.altertable.connections import AltertableConnectionManager
from dbt.adapters.sql import SQLAdapter


class AltertableAdapter(SQLAdapter):
    ConnectionManager = AltertableConnectionManager

    @classmethod
    def date_function(cls) -> str:
        return "now()"
