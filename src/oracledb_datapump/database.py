from __future__ import annotations

from datetime import datetime, timedelta, timezone
import functools
from collections.abc import Callable
from dataclasses import dataclass
from functools import cached_property
import operator
from time import perf_counter
from typing import Final, Iterable, TypeAlias, assert_never, cast
from zoneinfo import ZoneInfo

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
    start_ts = perf_counter()
    logger.debug("Acquiring connection...")

    if isinstance(params, Connection):
        connection = params
    elif isinstance(params, str):
        connection = connect(params, *args, **kwargs)
    elif isinstance(params, dict):
        connection = _Connection.from_dict(params)
    else:
        assert_never(params)

    logger.debug("Connection acquired. took %.4fs", (perf_counter() - start_ts))
    return connection


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


def get_db_timezone(connection: Connection) -> timezone:
    with connection.cursor() as cursor:
        cursor.execute(sql.SQL_GET_DB_TIMEZONE)
        result = cast(tuple[str], cursor.fetchone())
        tz_data = result[0]

    # Oracle can return either a timezone name or an offset.
    logger.debug("db timezone data: %s", tz_data)
    # Check if a timezone name was returned and parse it
    try:
        tz_info = ZoneInfo(tz_data)
        utc_offset = tz_info.utcoffset(datetime.utcnow())
        assert utc_offset
        tz = timezone(utc_offset)
        logger.debug("db timezone: %s", tz)
        return tz
    except Exception:
        pass
    # Assume timezone was returned as a UTC offset
    oper_fns = {"+": operator.pos, "-": operator.neg}
    offset_parts = list(tz_data)
    offset_oper = oper_fns[offset_parts.pop(0)]
    offset_hour = int("".join(offset_parts[0: offset_parts.index(":")]))
    offset_minute = int("".join(offset_parts[offset_parts.index(":") + 1:]))
    tz = timezone(offset_oper(timedelta(hours=offset_hour, minutes=offset_minute)))
    logger.debug("db timezone: %s", tz)
    return tz


def to_db_timezone(dt: datetime, connection: Connection) -> datetime:
    logger.debug("received datetime: %s", dt.isoformat())
    dt_with_db_tz = dt.astimezone(tz=get_db_timezone(connection))
    logger.debug("db timezone converted: %s", dt_with_db_tz.isoformat())
    return dt_with_db_tz


def dt_to_scn(dt: datetime, connection: Connection) -> int:
    with connection.cursor() as cursor:
        cursor.execute(sql.SQL_TIMESTAMP_TO_SCN, parameters=[dt])
        result = cast(tuple[int], cursor.fetchone())
        return int(result[0])
