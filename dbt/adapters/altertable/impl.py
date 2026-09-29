import os
import threading
from functools import cached_property
from typing import Any, ClassVar

from dbt.adapters.base import available
from dbt.adapters.base.impl import ConstraintSupport
from dbt.adapters.duckdb.constants import DUCKLAKE_ALTER_RENAME_FIX_VERSION
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.sql import SQLAdapter
from dbt_common.contracts.constraints import (
    ColumnLevelConstraint,
    ConstraintType,
    ModelLevelConstraint,
)
from dbt_common.exceptions import CompilationError, DbtDatabaseError
from packaging.version import Version

from dbt.adapters.altertable.connections import AltertableConnectionManager
from dbt.adapters.altertable.relation import AltertableRelation

logger = AdapterLogger("Altertable")


class AltertableAdapter(SQLAdapter):
    ConnectionManager = AltertableConnectionManager
    Relation = AltertableRelation

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.primary_key: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_SUPPORTED,
    }

    _warned_messages: ClassVar[set[str]] = set()
    _warned_lock: ClassVar[threading.Lock] = threading.Lock()

    def valid_incremental_strategies(self) -> list[str]:
        return ["append", "delete+insert", "merge", "microbatch"]

    @classmethod
    def date_function(cls) -> str:
        return "now()"

    @classmethod
    def process_parsed_constraint(
        cls,
        parsed_constraint: ColumnLevelConstraint | ModelLevelConstraint,
        render_func,
    ) -> str | None:
        if (
            parsed_constraint.type != ConstraintType.custom
            and cls.CONSTRAINT_SUPPORT[parsed_constraint.type] == ConstraintSupport.NOT_SUPPORTED
        ):
            raise CompilationError(
                f"{parsed_constraint.type.value} constraints are not supported on Altertable. "
                "Only not_null constraints can be enforced."
            )
        return super().process_parsed_constraint(parsed_constraint, render_func)

    @available
    def get_seed_file_path(self, model) -> str:
        return os.path.join(model["root_path"], model["original_file_path"])

    @available.parse(lambda relation: False)
    def is_ducklake(self, relation: AltertableRelation | None) -> bool:
        if relation is None or not relation.database:
            return False

        connection = self.connections.get_thread_connection().handle
        if connection.ducklake_catalog_names is None:
            _, cursor = self.connections.add_select_query(
                "select lower(database_name) from duckdb_databases() where type = 'ducklake'"
            )
            connection.ducklake_catalog_names = {row[0] for row in cursor.fetchall()}
        return relation.database.lower() in connection.ducklake_catalog_names

    @available.parse(lambda relation: False)
    def use_ducklake_table_workarounds(self, relation: AltertableRelation | None) -> bool:
        return self.is_ducklake(relation) and self.server_duckdb_version < Version(
            DUCKLAKE_ALTER_RENAME_FIX_VERSION
        )

    @available
    def is_motherduck(self) -> bool:
        return False

    @available
    def disable_transactions(self) -> bool:
        return False

    @available
    def parse_index(self, raw_index: Any) -> None:
        raise CompilationError(
            "Indexes are not supported on Altertable. Remove the `indexes` config from this model."
        )

    @available
    def warn_once(self, msg: str) -> None:
        with self._warned_lock:
            if msg in self._warned_messages:
                return
            self._warned_messages.add(msg)
        logger.warning(msg)

    @available
    def batch_id_for_model(self, model: Any) -> str:
        # Required by dbt-duckdb; only MotherDuck uses the batch suffix.
        return ""

    @cached_property
    def server_duckdb_version(self) -> Version:
        _, cursor = self.connections.add_select_query("select version()")
        row = cursor.fetchone()
        if not row or not row[0]:
            raise DbtDatabaseError("Unable to determine the Altertable DuckDB version")
        return Version(str(row[0]).lstrip("v"))
