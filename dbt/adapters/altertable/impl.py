import os

from dbt.adapters.base import available
from dbt.adapters.sql import SQLAdapter

from dbt.adapters.altertable.connections import AltertableConnectionManager


class AltertableAdapter(SQLAdapter):
    ConnectionManager = AltertableConnectionManager

    @classmethod
    def date_function(cls) -> str:
        return "now()"

    @available
    def get_seed_file_path(self, model) -> str:
        return os.path.join(model["root_path"], model["original_file_path"])
