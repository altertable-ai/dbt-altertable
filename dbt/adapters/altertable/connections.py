from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from types import TracebackType
from typing import TYPE_CHECKING, Any

import altertable_flightsql
import pyarrow as pa
from dbt.adapters.contracts.connection import (
    AdapterResponse,
    Connection,
    ConnectionState,
)
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.sql.connections import SQLConnectionManager
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.altertable.credentials import AltertableCredentials

if TYPE_CHECKING:
    import agate

logger = AdapterLogger("Altertable")


class AltertableCursor:
    """
    A PEP 249-compliant cursor wrapper around the altertable_flightsql Client.

    Results from the last ``execute()`` are kept as a ``pyarrow.Table``
    (``cursor.table``) and converted to Python tuples on demand in ``fetch*``.
    """

    def __init__(self, client: altertable_flightsql.Client) -> None:
        self._client = client
        self._table: pa.Table | None = None
        self._cursor_position: int = 0

    @property
    def table(self) -> pa.Table | None:
        """Arrow table from the last ``execute``, or ``None`` if no query has run."""
        return self._table

    @property
    def description(
        self,
    ) -> list[tuple[str, Any, None, None, None, None, None]] | None:
        """
        PEP 249: Sequence of 7-item sequences describing result columns.

        Each sequence contains: (name, type_code, display_size, internal_size,
        precision, scale, null_ok). We only populate name and type_code.
        """
        if self._table is None:
            return None
        return [
            (field.name, field.type, None, None, None, None, None) for field in self._table.schema
        ]

    @property
    def rowcount(self) -> int:
        """
        PEP 249: Number of rows affected by last execute.

        Returns -1 for SELECT statements or when count is unknown.
        """
        if self._table is None:
            return -1
        return self._table.num_rows

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
        self._table = None
        self._cursor_position = 0

        logger.debug("Executing SQL: %s", sql)
        if bindings is not None:
            logger.debug("With bindings: %s", bindings)
            with self._client.prepare(sql) as stmt:
                reader = stmt.query(parameters=bindings)
                self._table = reader.read_all()
        else:
            reader = self._client.query(sql)
            self._table = reader.read_all()

        return self

    def _slice_to_rows(self, offset: int, length: int) -> list[tuple[Any, ...]]:
        if self._table is None or length <= 0:
            return []
        chunk = self._table.slice(offset, length).to_pylist()
        return [tuple(row.values()) for row in chunk]

    def fetchone(self) -> tuple[Any, ...] | None:
        """
        Fetch the next row of a query result.

        Returns:
            A single row as a tuple, or None if no more rows.
        """
        if self._table is None or self._cursor_position >= self._table.num_rows:
            return None
        rows = self._slice_to_rows(self._cursor_position, 1)
        self._cursor_position += 1
        return rows[0]

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """
        Fetch the next set of rows.

        Args:
            size: Maximum number of rows to fetch. Defaults to arraysize=1 per PEP 249.

        Returns:
            List of rows as tuples.
        """
        if self._table is None:
            return []
        if size is None:
            size = 1
        end = min(self._cursor_position + size, self._table.num_rows)
        rows = self._slice_to_rows(self._cursor_position, end - self._cursor_position)
        self._cursor_position = end
        return rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        """
        Fetch all remaining rows.

        Returns:
            List of all remaining rows as tuples.
        """
        if self._table is None:
            return []
        remaining = self._table.num_rows - self._cursor_position
        rows = self._slice_to_rows(self._cursor_position, remaining)
        self._cursor_position = self._table.num_rows
        return rows

    def close(self) -> None:
        """Close the cursor and release the staged Arrow table."""
        self._table = None
        self._cursor_position = 0

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
            return cls._connect_client(connection.credentials)

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=1,
            retry_timeout=lambda attempt: attempt**2,
            retryable_exceptions=[Exception],
        )

    @classmethod
    def _connect_client(cls, credentials: AltertableCredentials) -> "AltertableConnection":
        client = altertable_flightsql.Client(
            username=credentials.username,
            password=credentials.password,
            catalog=None,
            schema=None,
            host=credentials.host,
            port=credentials.port,
            tls=credentials.tls,
        )
        return AltertableConnection(client)

    @classmethod
    def get_response(cls, cursor: AltertableCursor) -> AdapterResponse:
        return AdapterResponse(_message="OK")

    @classmethod
    def get_result_from_cursor(cls, cursor: AltertableCursor, limit: int | None) -> "agate.Table":
        """Build an ``agate.Table`` from ``cursor.table``, applying ``limit`` if given."""
        from dbt_common.clients.agate_helper import empty_table, table_from_data_flat

        table = cursor.table
        if table is None:
            return empty_table()
        if limit is not None and table.num_rows > limit:
            table = table.slice(0, limit)
        column_names = list(table.schema.names)
        return table_from_data_flat(table.to_pylist(), column_names)
