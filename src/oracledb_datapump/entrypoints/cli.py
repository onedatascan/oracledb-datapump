import argparse
import json
import logging
import os
import sys
from datetime import datetime
from typing import TYPE_CHECKING, Literal

from oracledb_datapump import constants
from oracledb_datapump.base import ConnectDict
from oracledb_datapump.client import DataPump
from oracledb_datapump.exceptions import BadRequest, UsageError
from oracledb_datapump.log import DEFAULT_LOG_FMT, get_logger
from oracledb_datapump.util import parse_dt

if TYPE_CHECKING:
    from oracledb_datapump.request import JobDirective

logger = get_logger(__name__)


def main() -> int:
    """CLI wrapper for oracledb_datapump package"""

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format=DEFAULT_LOG_FMT,
    )

    if sys.version_info < (3, 11):
        raise RuntimeError("Requires python>=3.11.0")

    parser = argparse.ArgumentParser(
        description="Remote Oracle Datapump (limited feature set)"
    )
    parser.add_argument("op_mode", choices=["import", "export", "impdp", "expdp"])

    job_mode = parser.add_mutually_exclusive_group(required=True)
    job_mode.add_argument("--schema", action="append", default=[])
    job_mode.add_argument("--full", action="store_true")
    job_mode.add_argument("--table", action="append", default=[])

    parser.add_argument("--user", required=True, help="Oracle admin user")
    parser.add_argument("--password", required=True, help="Oracle admin password")
    parser.add_argument("--host", required=True, help="Database service host")
    parser.add_argument("--database", required=True, help="Database service name")
    parser.add_argument(
        "--parallel", default=1, help="Number of datapump workers", type=int
    )
    parser.add_argument(
        "--dumpfile",
        action="append",
        default=[],
        help="Oracle dumpfile - Required for import",
    )
    parser.add_argument(
        "--compression", choices=["DATA_ONLY", "METADATA_ONLY", "ALL", "NONE"]
    )
    parser.add_argument(
        "--exclude", action="append", default=[], help="Exclude object type"
    )
    parser.add_argument(
        "--remap_schema",
        action="append",
        default=[],
        help="Remap schema FROM_SCHEMA:TO_SCHEMA",
    )
    parser.add_argument(
        "--remap_tablespace",
        action="append",
        default=[],
        help="Remap tablespace FROM_TBLSPC:TO_TBLSPC",
    )
    parser.add_argument(
        "--flashback_time", default=None, help="ISO format timestamp"
    )
    parser.add_argument(
        "--directive", action="append", default=[], help="Datapump directive NAME:VALUE"
    )

    args = parser.parse_args()

    op_map = {
        "import": "IMPORT",
        "impdp": "IMPORT",
        "export": "EXPORT",
        "expdp": "EXPORT",
    }

    operation: str = op_map[args.op_mode.lower()]

    if args.schema:
        mode = "SCHEMA"
    elif args.table:
        mode = "TABLE"
    else:
        mode = "FULL"

    if operation == "import" and not args.dumpfile:
        raise BadRequest("--dumpfile argument is required for IMPORT!")

    dumpfile: list[str] = [str(i) for i in args.dumpfile]

    directives = parse_directives(
        parallel=args.parallel,
        compression=args.compression,
        schemas=args.schema,
        tables=args.table,
        exclude=args.exclude,
        remap_schema=args.remap_schema,
        remap_tablespace=args.remap_tablespace,
        flashback_time=args.flashback_time,
        directives=args.directive,
    )

    payload = {
        "operation": operation.upper(),
        "mode": mode,
        "wait": True,
        "dumpfiles": dumpfile,
        "directives": directives,
    }
    logger.info(payload)

    connect_dict: ConnectDict = {
        "user": str(args.user),
        "password": str(args.password),
        "host": str(args.host),
        "database": str(args.database),
    }

    request = {"connection": connect_dict, "request": "SUBMIT", "payload": payload}

    response = DataPump.submit(json.dumps(request))
    logfile = DataPump.get_logfile(
        logfile=str(response.logfile),
        connection=connect_dict,
    )

    if response.state == "COMPLETED":
        logger.info(response)
        print(logfile, file=sys.stderr)
        return 0
    else:
        logger.error(response)
        print(logfile, file=sys.stderr)
        return 1


def parse_directives(
    parallel: int = 1,
    compression: Literal["DATA_ONLY", "METADATA_ONLY", "ALL", "NONE"] | None = None,
    schemas: list[str] | None = None,
    tables: list[str] | None = None,
    exclude: list[str] | None = None,
    remap_schema: list[str] | None = None,
    remap_tablespace: list[str] | None = None,
    flashback_time: datetime | str | None = None,
    directives: list[str] | None = None,
) -> list["JobDirective"]:

    job_directives: list["JobDirective"] = [{"name": "PARALLEL", "value": parallel}]

    if compression:
        job_directives.append({"name": "COMPRESSION", "value": compression})

    if schemas:
        job_directives.extend([{"name": "INCLUDE_SCHEMA", "value": s} for s in schemas])
    if tables:
        job_directives.extend([{"name": "INCLUDE_TABLE", "value": t} for t in tables])
    if exclude:
        job_directives.extend(
            [{"name": "EXCLUDE_OBJECT_TYPE", "value": e} for e in exclude]
        )

    if remap_schema:
        for rs in remap_schema:
            parts = rs.split(constants.ARG_DELIMITER)
            if len(parts) == 2:
                job_directives.append(
                    {
                        "name": "REMAP_SCHEMA",
                        "old_value": parts[0],
                        "value": parts[1],
                    }
                )
            else:
                raise UsageError(
                    f"Invalid remap arg {rs}. Should be a colon delimited string"
                    " FROM_VALUE:TO_VALUE"
                )
    if remap_tablespace:
        for rt in remap_tablespace:
            parts = rt.split(constants.ARG_DELIMITER)
            if len(parts) == 2:
                job_directives.append(
                    {
                        "name": "REMAP_TABLESPACE",
                        "old_value": parts[0],
                        "value": parts[1],
                    }
                )
            else:
                raise UsageError(
                    f"Invalid remap arg {rt}. Should be a colon delimited string"
                    " FROM_VALUE:TO_VALUE"
                )

    if flashback_time:
        dt: datetime = parse_dt(flashback_time)
        job_directives.append({"name": "FLASHBACK_TIME", "value": dt.isoformat()})

    if directives:
        for d in directives:
            parts = d.split(constants.ARG_DELIMITER)
            if len(parts) == 2:
                job_directives.append({"name": parts[0].upper(), "value": parts[1]})
            else:
                raise UsageError(
                    f"Unsupported directive arg {d}. Should be a colon delimited"
                    " string NAME:VALUE"
                )

    return job_directives
