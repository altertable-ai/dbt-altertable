from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from types import TracebackType
from typing import Annotated, Any

import altertable_flightsql
import pyarrow as pa
from dbt.adapters.contracts.connection import (
    AdapterResponse,
    Connection,
    ConnectionState,
    Credentials,
)
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.sql.connections import SQLConnectionManager
from dbt_common.exceptions import DbtRuntimeError
from mashumaro.jsonschema.annotations import Maximum, Minimum

logger = AdapterLogger("Altertable")


class AltertableCursor:
    """
    A PEP 249-compliant cursor wrapper around the altertable_flightsql Client.

    This cursor provides the standard DB-API 2.0 interface that dbt expects,
    translating calls to the altertable_flightsql client methods.
    """

    def __init__(self, client: altertable_flightsql.Client) -> None:
        self._client = client
        self._results: list[tuple[Any, ...]] | None = None
        self._description: list[tuple[str, Any, None, None, None, None, None]] | None = None
        self._rowcount: int = -1
        self._cursor_position: int = 0

    @property
    def description(
        self,
    ) -> list[tuple[str, Any, None, None, None, None, None]] | None:
        """
        PEP 249: Sequence of 7-item sequences describing result columns.

        Each sequence contains: (name, type_code, display_size, internal_size,
        precision, scale, null_ok). We only populate name and type_code.
        """
        return self._description

    @property
    def rowcount(self) -> int:
        """
        PEP 249: Number of rows affected by last execute.

        Returns -1 for SELECT statements or when count is unknown.
        """
        return self._rowcount

    def execute(
        self,
        sql: str,
        bindings: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> "AltertableCursor":
        """
        Execute a SQL statement.

        Args:
            sql: SQL statement to execute.
            bindings: Optional parameter bindings. Can be a sequence (positional)
                or a mapping (named parameters).

        Returns:
            Self for method chaining.
        """
        self._results = None
        self._description = None
        self._rowcount = -1
        self._cursor_position = 0

        logger.debug("Executing SQL: %s", sql)
        if bindings is not None:
            logger.debug("With bindings: %s", bindings)
            with self._client.prepare(sql) as stmt:
                reader = stmt.query(parameters=bindings)
                table = reader.read_all()
        else:
            reader = self._client.query(sql)
            table = reader.read_all()

        self._process_arrow_table(table)
        return self

    def _process_arrow_table(self, table: pa.Table) -> None:
        """Process an Arrow table into cursor results."""
        self._description = [
            (field.name, field.type, None, None, None, None, None) for field in table.schema
        ]

        columns = list(table.to_pydict().values())
        if columns:
            num_rows = len(columns[0])
            self._results = [tuple(col[i] for col in columns) for i in range(num_rows)]
        else:
            self._results = []

        self._rowcount = len(self._results)

    def fetchone(self) -> tuple[Any, ...] | None:
        """
        Fetch the next row of a query result.

        Returns:
            A single row as a tuple, or None if no more rows.
        """
        if self._results is None or self._cursor_position >= len(self._results):
            return None
        row = self._results[self._cursor_position]
        self._cursor_position += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """
        Fetch the next set of rows.

        Args:
            size: Maximum number of rows to fetch. Defaults to arraysize=1 per PEP 249.

        Returns:
            List of rows as tuples.
        """
        if self._results is None:
            return []
        if size is None:
            size = 1
        end = min(self._cursor_position + size, len(self._results))
        rows = self._results[self._cursor_position : end]
        self._cursor_position = end
        return rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        """
        Fetch all remaining rows.

        Returns:
            List of all remaining rows as tuples.
        """
        if self._results is None:
            return []
        rows = self._results[self._cursor_position :]
        self._cursor_position = len(self._results)
        return rows

    def close(self) -> None:
        """Close the cursor and release any staged results."""
        self._results = None
        self._description = None

    def __iter__(self) -> "AltertableCursor":
        """Allow iteration over results."""
        return self

    def __next__(self) -> tuple[Any, ...]:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row


class AltertableConnection:
    """
    A PEP 249-compliant connection wrapper around the altertable_flightsql Client.

    This provides the connection interface that dbt expects, with a cursor() method
    that returns AltertableCursor instances.
    """

    def __init__(self, client: altertable_flightsql.Client) -> None:
        self._client = client

    def __enter__(self) -> "AltertableConnection":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def cursor(self) -> AltertableCursor:
        """Create a new cursor for this connection."""
        return AltertableCursor(self._client)

    def close(self) -> None:
        """Close the connection."""
        self._client.close()

    def commit(self) -> None:
        """Commit current transaction (no-op for now as we use auto-commit)."""

    def rollback(self) -> None:
        """Rollback current transaction (no-op for now as we use auto-commit)."""


@dataclass
class AltertableCredentials(Credentials):
    username: str
    password: str
    host: str = "flight.altertable.ai"
    port: Annotated[int, Minimum(0), Maximum(65535)] = 443
    tls: bool = True

    @property
    def type(self) -> str:
        return "altertable"

    @property
    def unique_field(self) -> str:
        return "host"

    def _connection_keys(self) -> tuple[str, ...]:
        return (
            "username",
            "password",
            "database",
            "schema",
            "host",
            "port",
            "tls",
        )


class AltertableConnectionManager(SQLConnectionManager):
    TYPE = "altertable"

    @contextmanager
    def exception_handler(self, sql: str) -> Iterator[None]:
        try:
            yield

        except Exception as e:
            logger.error(f"Error executing SQL: {sql}")
            raise DbtRuntimeError(str(e)) from e

    def cancel(self, connection: Connection) -> None:
        logger.debug(f"Attempting to cancel connection: {connection.name}")

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            return connection

        def connect() -> AltertableConnection:
            client = altertable_flightsql.Client(
                username=connection.credentials.username,
                password=connection.credentials.password,
                catalog=connection.credentials.database,
                schema=connection.credentials.schema,
                host=connection.credentials.host,
                port=connection.credentials.port,
                tls=connection.credentials.tls,
            )
            return AltertableConnection(client)

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=1,
            retry_timeout=lambda attempt: attempt**2,
            retryable_exceptions=[Exception],
        )

    @classmethod
    def get_response(cls, cursor: AltertableCursor) -> AdapterResponse:
        return AdapterResponse(_message="OK")
