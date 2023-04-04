import functools
import json
from datetime import datetime
from math import log10
from typing import Any, Callable

import oracledb

from oracledb_datapump import constants
from oracledb_datapump.exceptions import UsageError
from oracledb_datapump.log import get_logger

logger = get_logger(__name__)


def string_shortener(s: str, max_: int) -> str:
    """Truncates a string k8s style."""

    if s is None or len(s) <= max_:
        return s
    if max_ <= 2 or len(s) <= 2:
        return s[:max_]

    start, mid, end = s[0], s[1:-1], s[-1]
    n_over = len(s) - max_
    n_drop = n_over + len(str(n_over))
    keep = mid[:-n_drop]
    over = int(log10(n_drop)) - int(log10(n_over))

    if over > 0:
        keep = keep[:-over]
    short = start + keep + str(n_drop) + end

    if len(short) > max_:
        return string_shortener(short, max_)

    return short


def object_loads(obj: oracledb.Object) -> dict | list | None:
    """Loads an Oracle object type into a dict or list"""
    if obj.type.iscollection:
        retval = []
        for value in obj.aslist():
            if isinstance(value, oracledb.Object):
                value = object_loads(value)
            retval.append(value.lower() if isinstance(value, str) else value)
    else:
        retval = {}
        for attr in obj.type.attributes:
            value = getattr(obj, attr.name)
            if value is None:
                continue
            if isinstance(value, oracledb.Object):
                value = object_loads(value)
            retval[attr.name.lower()] = value
    return retval


def str_or_kv(x: str | tuple[str, str]) -> tuple[str, str | None]:
    if isinstance(x, str):
        return x, None
    elif isinstance(x, tuple):
        return x


def parse_dt(s: str | datetime) -> datetime:
    if isinstance(s, datetime):
        return s
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        raise ValueError(f"Invalid ISO datetime string: {s}")


def parse_colon_delimited(val):
    try:
        return dict([tuple(val.split(":"))])
    except ValueError:
        UsageError(
            f"Datapump arguments must be colon delimited! NAME:VALUE Received: {val}",
        )


def requires_db_ctx(method: Callable):
    @functools.wraps(method)
    def decorator(self, *args, **kwargs):
        if getattr(self, "database_ctx", None) is None:
            raise ValueError(
                f"Database context must be set on {self.__class__.__name__}"
                f" to call {self.__class__.__name__}.{method.__name__}()!"
            )
        return method(self, *args, **kwargs)

    return decorator


class JsonSerializer(json.JSONEncoder):
    def default(self, o: object) -> str | Any:
        if isinstance(o, datetime):
            return o.strftime(constants.DATE_STR_FMT)
        return super().default(o)
