from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Tuple

import adbc_driver_flightsql
import adbc_driver_flightsql.dbapi
import adbc_driver_manager
from dbt_common.exceptions import DbtRuntimeError
from mashumaro.jsonschema.annotations import Maximum, Minimum
from typing_extensions import Annotated

from dbt.adapters.contracts.connection import (
    AdapterResponse,
    Connection,
    ConnectionState,
    Credentials,
)
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.sql.connections import SQLConnectionManager

logger = AdapterLogger("Altertable")


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

    def _adbc_uri(self) -> str:
        scheme = "grpc+tls" if self.tls else "grpc"
        return f"{scheme}://{self.host}:{self.port}"

    def _adbc_options(self) -> Dict[str, Any]:
        options = {
            "db_kwargs": {
                adbc_driver_manager.DatabaseOptions.USERNAME.value: self.username,
                adbc_driver_manager.DatabaseOptions.PASSWORD.value: self.password,
            },
            "conn_kwargs": {
                # adbc_driver_manager.ConnectionOptions.CURRENT_CATALOG.value: self.database,
                # adbc_driver_manager.ConnectionOptions.CURRENT_DB_SCHEMA.value: self.schema,
            },
        }

        return options

    def _connection_keys(self) -> Tuple[str, ...]:
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
    def exception_handler(self, sql: str):
        try:
            yield

        except Exception as e:
            logger.error(f"Error executing SQL: {sql}")
            raise DbtRuntimeError(e) from e

    def cancel(self, connection: Connection) -> None:
        logger.debug(f"Attempting to cancel connection: {connection.name}")

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            return connection

        def connect():
            uri = connection.credentials._adbc_uri()
            options = connection.credentials._adbc_options()
            # Enable autocommit to prevent ADBC from managing transactions
            # This allows dbt to handle transactions at the SQL level
            return adbc_driver_flightsql.dbapi.connect(uri, autocommit=True, **options)

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=1,
            retry_timeout=lambda attempt: attempt**2,
            retryable_exceptions=[Exception],
        )

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        return AdapterResponse(_message="OK")
