from __future__ import annotations

import functools
from collections.abc import Callable
from dataclasses import dataclass
from functools import cached_property
from typing import Final, Iterable, TypeAlias, assert_never

import oracledb

from oracledb_datapump import constants, sql
from oracledb_datapump.base import ConnectDict
from oracledb_datapump.exceptions import DatabaseError, UsageError
from oracledb_datapump.log import get_logger

logger = get_logger(__name__)


# oracledb package has some issues with its __init__.py
# pyright: reportPrivateImportUsage=false

DB_TYPE: TypeAlias = oracledb.var.DbType
DB_OBJECT_TYPE: TypeAlias = oracledb.ObjectType
DB_OBJECT: TypeAlias = oracledb.OBJECT
DB_LOB: TypeAlias = oracledb.LOB
DB_CLOB: TypeAlias = oracledb.CLOB
DB_BLOB: TypeAlias = oracledb.BLOB

AUTH_MODE_SYSDBA: Final[int] = oracledb.AUTH_MODE_SYSDBA
AUTH_MODE_DEFAULT: Final[int] = oracledb.AUTH_MODE_DEFAULT

Connection: TypeAlias = oracledb.Connection
Cursor: TypeAlias = oracledb.Cursor


class _Connection(Connection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._password = ""
        self._host = self.dsn.split("/")[0]
        self._database = self.dsn.split("/")[1]

    def __str__(self):
        return (
            f"DatabaseContext(id={id(self)}, user={self.username}, "
            f"host={self._host}, database={self._database})"
        )

    @classmethod
    def from_dict(cls, cd: ConnectDict):
        return connect(
            user=cd["user"],
            password=cd["password"],
            dsn=f"{cd['host']}/{cd['database']}",
        )


# oracledb.connect function typing doesn't property communicate the usage of conn_class
connect: Callable[..., _Connection] = functools.partial(
    oracledb.connect, conn_class=_Connection
)  # type: ignore


def get_connection(
    params: str | ConnectDict | Connection, /, *args, **kwargs
) -> Connection:
    if isinstance(params, Connection):
        return params
    if isinstance(params, str):
        return connect(params, *args, **kwargs)
    if isinstance(params, dict):
        return _Connection.from_dict(params)
    else:
        assert_never(params)


@dataclass
class Tablespace:
    name: str


@dataclass
class Schema:
    name: str

    def __post_init__(self):
        self.name = self.name.upper()
        self.handler: SchemaHandler | None = None

    def get_handler(self, connection: Connection):
        if not self.handler:
            self.handler = SchemaHandler(self, connection)

    @cached_property
    def tablespaces(self) -> Iterable[Tablespace]:
        if not self.handler:
            raise UsageError("Cannot fetch schema tablespaces without schema handler!")
        return self.handler.get_tablespaces()

    @cached_property
    def schema_version(self) -> str | None:
        if not self.handler:
            raise UsageError("Cannot fetch schema version without schema handler!")
        return self.handler.get_version()


class SchemaHandler:
    def __init__(self, schema: Schema, connection: Connection):
        self.schema = schema
        self.connection = connection

    def get_tablespaces(self) -> Iterable[Tablespace]:
        with self.connection.cursor() as cursor:
            cursor.execute(sql.SQL_GET_SCHEMA_TABLESPACES, [self.schema.name])
            return [Tablespace(row[0]) for row in cursor]

    def get_version(self) -> str | None:
        formatted_sql = sql.SQL_GET_FLYWAY_SCHEMA_VERSION.format(
            constants.FLYWAY_SCHEMA_HISTORY_ENV
        )
        # Warning this uses SQL string concat because the table name is dynamic
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(formatted_sql)
                return next(iter(cursor.fetchall()), [""])[0]
        except DatabaseError:
            return None
