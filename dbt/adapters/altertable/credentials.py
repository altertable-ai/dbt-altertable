from dataclasses import dataclass
from typing import Annotated

from dbt.adapters.contracts.connection import Credentials
from mashumaro.jsonschema.annotations import Maximum, Minimum


@dataclass(kw_only=True)
class AltertableCredentials(Credentials):
    username: str
    password: str
    database: str | None = None
    schema: str | None = None
    host: str = "flight.altertable.ai"
    port: Annotated[int, Minimum(0), Maximum(65535)] = 443
    tls: bool = True

    _ALIASES = {"catalog": "database"}

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
