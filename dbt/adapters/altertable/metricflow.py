from __future__ import annotations

from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import AdapterBackedSqlClient
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.duckdb_renderer import DuckDbSqlPlanRenderer

# dbt-metricflow only accepts a hardcoded list of 7 adapters and crashes on anything else.
# This patch intercepts the initialization and tells it to use DuckDB's SQL renderer,
# since Altertable runs on DuckLake which uses DuckDB SQL dialect.
_original_init = AdapterBackedSqlClient.__init__


def _patched_init(self, adapter) -> None:
    if adapter.type() == "altertable":
        self._adapter = adapter
        self._sql_engine_type = SqlEngine.DUCKDB
        self._sql_plan_renderer = DuckDbSqlPlanRenderer()
    else:
        _original_init(self, adapter)


AdapterBackedSqlClient.__init__ = _patched_init  # ty: ignore[invalid-assignment]
