from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from types import TracebackType
from typing import TYPE_CHECKING, Any

import altertable_flightsql
import pyarrow as pa
import pyarrow.types as patypes
from altertable_flightsql.client import IngestTableMode
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
    from altertable_flightsql.client import Transaction

logger = AdapterLogger("Altertable")

SESSION_EXPIRED_MESSAGE = "Session expired"


def _split_to_message_size(
    batch: pa.RecordBatch, max_bytes: int = 1024 * 1024
) -> Iterator[pa.RecordBatch]:
    """Halve ``batch`` until each part is at most ``max_bytes`` or a single row.

    Each part is sent as one gRPC message, and servers reject messages above their cap
    (4 MiB on altertable-mock), so a large result cannot be sent as a single batch.
    """
    if batch.nbytes <= max_bytes or batch.num_rows <= 1:
        yield batch
        return
    middle = batch.num_rows // 2
    yield from _split_to_message_size(batch.slice(0, middle), max_bytes)
    yield from _split_to_message_size(batch.slice(middle), max_bytes)


def _is_session_expired(exc: BaseException) -> bool:
    """True when the server no longer knows the Flight session (e.g. it expired while idle)."""
    return isinstance(exc, pa.ArrowException) and SESSION_EXPIRED_MESSAGE in str(exc)


def _normalize_flight_sql_scalar(value: Any) -> Any:
    """Coerce dbt/agate values into types Arrow Flight can bind (e.g. Decimal → int)."""
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return value
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    return value


def _arrow_type_to_column_dtype(data_type: pa.DataType) -> str:
    """Map Arrow types from ``cursor.description`` to dbt ``Column`` dtype strings (DuckDB-like)."""
    if patypes.is_boolean(data_type):
        return "BOOLEAN"
    if patypes.is_int8(data_type) or patypes.is_uint8(data_type):
        return "SMALLINT"
    if patypes.is_int16(data_type):
        return "SMALLINT"
    if patypes.is_uint16(data_type):
        return "INTEGER"
    if patypes.is_int32(data_type):
        return "INTEGER"
    if patypes.is_uint32(data_type):
        return "BIGINT"
    if patypes.is_int64(data_type):
        return "BIGINT"
    if patypes.is_uint64(data_type):
        return "UBIGINT"
    if patypes.is_float32(data_type):
        return "REAL"
    if patypes.is_float64(data_type):
        return "DOUBLE"
    if patypes.is_decimal(data_type):
        return f"DECIMAL({data_type.precision}, {data_type.scale})"
    if patypes.is_timestamp(data_type):
        return "TIMESTAMP"
    if patypes.is_date32(data_type) or patypes.is_date64(data_type):
        return "DATE"
    if patypes.is_time32(data_type) or patypes.is_time64(data_type):
        return "TIME"
    if patypes.is_string(data_type) or patypes.is_large_string(data_type):
        return "VARCHAR"
    if patypes.is_binary(data_type) or patypes.is_large_binary(data_type):
        return "BLOB"
    if patypes.is_null(data_type):
        return "VARCHAR"
    return "VARCHAR"


def _normalize_flight_sql_parameters(
    bindings: Sequence[Any] | Mapping[str, Any],
) -> Sequence[Any] | Mapping[str, Any]:
    """
    Coerce all parameters before ``PreparedStatement.query``.

    dbt seeds and other paths may supply ``decimal.Decimal`` (agate) and ``datetime``;
    the Flight/Arrow path rejects those for integer columns (e.g. ``INT32``).
    """
    if isinstance(bindings, Mapping):
        out: dict[str, Any] = {str(k): _normalize_flight_sql_scalar(v) for k, v in bindings.items()}
        return out
    return [_normalize_flight_sql_scalar(x) for x in bindings]


class AltertableCursor:
    """
    A PEP 249-compliant cursor wrapper around the altertable_flightsql Client.

    Results from the last ``execute()`` are kept as a ``pyarrow.Table``
    (``cursor.table``) and converted to Python tuples on demand in ``fetch*``.
    """

    def __init__(
        self,
        client: altertable_flightsql.Client,
        transaction: "Transaction | None" = None,
        reconnect: Callable[[], altertable_flightsql.Client] | None = None,
    ) -> None:
        self._client = client
        self._transaction = transaction
        self._reconnect = reconnect
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

        try:
            self._table = self._execute_to_table(sql, bindings)
        except Exception as e:
            # An expired session is rejected before anything runs, so retrying is safe unless the
            # statement belonged to a transaction that died with the session.
            if (
                self._transaction is not None
                or self._reconnect is None
                or not _is_session_expired(e)
            ):
                raise
            self._client = self._reconnect()
            self._table = self._execute_to_table(sql, bindings)

        return self

    def _execute_to_table(
        self, sql: str, bindings: Sequence[Any] | Mapping[str, Any] | None
    ) -> pa.Table:
        if bindings is not None:
            params = _normalize_flight_sql_parameters(bindings)
            with self._client.prepare(sql, transaction=self._transaction) as stmt:
                return stmt.query(parameters=params).read_all()
        return self._client.query(sql, transaction=self._transaction).read_all()

    def _slice_to_rows(self, offset: int, length: int) -> list[tuple[Any, ...]]:
        if self._table is None or length <= 0:
            return []
        chunk = self._table.slice(offset, length)
        if chunk.num_columns == 0:
            return [()] * chunk.num_rows
        return list(zip(*(column.to_pylist() for column in chunk.itercolumns()), strict=True))

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

    def __init__(
        self,
        client: altertable_flightsql.Client,
        connect: Callable[[], altertable_flightsql.Client] | None = None,
    ) -> None:
        self._client = client
        self._connect = connect
        self._transaction: Transaction | None = None
        self.ducklake_catalog_names: set[str] | None = None

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
        reconnect = self._reconnect if self._connect is not None else None
        return AltertableCursor(self._client, self._transaction, reconnect=reconnect)

    def ingest(self, table: pa.Table, catalog: str, schema: str, name: str) -> None:
        """Create ``catalog.schema.name`` from ``table`` with Flight SQL bulk ingest.

        An empty table still sends one empty batch: a stream without batches creates no table.
        """
        batches = table.to_batches() or [pa.RecordBatch.from_pylist([], schema=table.schema)]
        with self._client.ingest(
            table_name=name,
            schema_name=schema,
            catalog_name=catalog,
            schema=table.schema,
            mode=IngestTableMode.CREATE,
            transaction=self._transaction,
        ) as writer:
            for batch in batches:
                for part in _split_to_message_size(batch):
                    writer.write_batch(part)

    def _can_reconnect(self, exc: Exception) -> bool:
        return self._connect is not None and _is_session_expired(exc)

    def _reconnect(self) -> altertable_flightsql.Client:
        """Replace the expired Flight session with a new client and return it."""
        assert self._connect is not None
        logger.info("Flight session expired; opening a new session")
        try:
            self._client.close()
        except Exception as e:
            logger.debug(f"Failed to close expired Flight session: {e}")
        self._client = self._connect()
        return self._client

    def begin(self) -> None:
        if self._transaction is not None:
            return
        try:
            self._transaction = self._client.begin_transaction()
        except Exception as e:
            if not self._can_reconnect(e):
                raise
            self._reconnect()
            self._transaction = self._client.begin_transaction()

    def close(self) -> None:
        try:
            self._client.close()
        except Exception as e:
            logger.warning(f"Failed to close Flight session: {e}")

    def commit(self) -> None:
        if self._transaction is not None:
            self._client.commit_transaction(self._transaction)
            self._transaction = None

    def rollback(self) -> None:
        if self._transaction is None:
            return
        transaction = self._transaction
        self._transaction = None
        try:
            self._client.rollback_transaction(transaction)
        except Exception as e:
            # Nothing to roll back: the transaction died with the session.
            if not self._can_reconnect(e):
                raise
            self._reconnect()


class AltertableConnectionManager(SQLConnectionManager):
    TYPE = "altertable"

    @classmethod
    def data_type_code_to_name(cls, type_code: Any) -> str:
        """PEP 249 uses int codes; our cursor uses Arrow ``DataType`` instances."""
        if isinstance(type_code, pa.DataType):
            return _arrow_type_to_column_dtype(type_code)
        if isinstance(type_code, str):
            return type_code
        if isinstance(type_code, int):
            return "VARCHAR"
        return "VARCHAR"

    @contextmanager
    def exception_handler(self, sql: str) -> Iterator[None]:
        try:
            yield

        except Exception as e:
            logger.exception(f"Error executing SQL ({type(e).__name__}): {e}")
            self.rollback_if_open()
            raise DbtRuntimeError(str(e)) from e

    def cancel(self, connection: Connection) -> None:
        logger.debug(f"Attempting to cancel connection: {connection.name}")

    def begin(self):
        connection = self.get_thread_connection()
        connection.handle.begin()
        connection.transaction_open = True
        return connection

    def commit(self):
        connection = self.get_thread_connection()
        connection.handle.commit()
        connection.transaction_open = False
        return connection

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: int | None = None,
    ):
        if sql.strip().rstrip(";").strip().casefold() == "commit":
            from dbt_common.clients.agate_helper import empty_table

            self.commit()
            return AdapterResponse(_message="OK"), empty_table()

        return super().execute(sql, auto_begin=auto_begin, fetch=fetch, limit=limit)

    def release(self) -> None:
        self.rollback_if_open()

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
        def new_client() -> altertable_flightsql.Client:
            return altertable_flightsql.Client(
                username=credentials.username,
                password=credentials.password,
                catalog=None,
                schema=None,
                host=credentials.host,
                port=credentials.port,
                tls=credentials.tls,
                compute_size=credentials.compute_size,
            )

        return AltertableConnection(new_client(), connect=new_client)

    @classmethod
    def get_response(cls, cursor: AltertableCursor) -> AdapterResponse:
        return AdapterResponse(_message="OK")

    @classmethod
    def get_result_from_cursor(cls, cursor: AltertableCursor, limit: int | None) -> "agate.Table":
        """Build an ``agate.Table`` from ``cursor.table``, applying ``limit`` if given."""
        from agate.utils import deduplicate
        from dbt_common.clients.agate_helper import empty_table, table_from_data_flat

        table = cursor.table
        if table is None:
            return empty_table()
        if limit is not None and table.num_rows > limit:
            table = table.slice(0, limit)
        column_names = deduplicate(table.schema.names, column_names=True)
        return table_from_data_flat(table.rename_columns(column_names).to_pylist(), column_names)
