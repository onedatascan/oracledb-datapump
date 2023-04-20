from __future__ import annotations

import json
import pprint
from time import perf_counter
from typing import TYPE_CHECKING, Iterable, cast

from oracledb_datapump import constants, sql
from oracledb_datapump.base import (
    DataPumpHandle,
    JobMetaData,
    JobMode,
    Operation,
    Stage,
)
from oracledb_datapump.database import Connection
from oracledb_datapump.exceptions import JobNotFound
from oracledb_datapump.files import LogFile, OracleFile, ora_open
from oracledb_datapump.log import get_logger
from oracledb_datapump.status import (
    JobState,
    JobStatusInfo,
    JobStatusRequestType,
    get_job_status,
    get_status_on_exception,
)
from oracledb_datapump.util import JsonSerializer

if TYPE_CHECKING:
    from oracledb_datapump.context import JobContext, OpenContext, StatusContext
    from oracledb_datapump.directives import DirectiveBase


logger = get_logger(__name__)


# Initialization commands - called by initializers


@get_status_on_exception
def open_datapump(
    job_name: str,
    operation: Operation,
    mode: JobMode,
    parallel: int,
    connection: Connection,
) -> DataPumpHandle:
    logger.info(
        "Opening job_name: %s oper: %s mode: %s parallel: %d",
        job_name,
        operation,
        mode,
        parallel,
        ctx=connection,
    )
    with connection.cursor() as cursor:
        job_handle = cursor.callfunc(
            name="DBMS_DATAPUMP.OPEN",
            return_type=int,
            keyword_parameters={
                "operation": str(operation),
                "job_mode": str(mode),
                "job_name": job_name,
            },
        )
    return DataPumpHandle(cast(int, job_handle))


@get_status_on_exception
def attach_datapump(
    job_name: str,
    job_owner: str,
    connection: Connection,
) -> DataPumpHandle:
    logger.info(
        "Attaching job_name: %s job_owner: %s", job_name, job_owner, ctx=connection
    )

    if not job_exists(job_name, job_owner, connection):
        raise JobNotFound("Job not found!", job_name, job_owner)

    with connection.cursor() as cursor:
        job_handle = cursor.callfunc(
            name="DBMS_DATAPUMP.ATTACH",
            return_type=int,
            parameters=[job_name, job_owner],
        )

    return DataPumpHandle(cast(int, job_handle))


# Datapump commands - Run by context handlers.


class GetStatus:
    def __init__(
        self,
        request_type: JobStatusRequestType | None,
        timeout: int | None,
        logfile: LogFile | str | None,
    ):
        self.request_type = request_type
        self.timeout = timeout
        self.logfile = logfile

    def __call__(self, ctx: JobContext | StatusContext) -> JobStatusInfo:
        logger.info("Getting job status.", ctx=ctx)
        if self.timeout is None:
            self.timeout = -1
        return get_job_status(ctx, self.request_type, self.timeout, self.logfile)


class Start:
    @get_status_on_exception
    def __call__(self, ctx: OpenContext) -> None:
        logger.info("Starting job.", ctx=ctx)

        with ctx.connection.cursor() as cursor:
            cursor.callproc("DBMS_DATAPUMP.START_JOB", [int(ctx.job_handle)])


class Detach:
    @get_status_on_exception
    def __call__(self, ctx: JobContext) -> None:
        logger.info("Detaching job.", ctx=ctx)
        if not job_exists(ctx.job_name, ctx.job_owner, ctx.connection):
            logger.debug(
                "Job(job_name=%s, job_owner=%s) not found",
                ctx.job_name,
                ctx.job_owner,
                ctx=ctx.connection,
            )
            return

        with ctx.connection.cursor() as cursor:
            cursor.callproc("DBMS_DATAPUMP.DETACH", [int(ctx.job_handle)])


class WaitOnCompletion:
    @get_status_on_exception
    def __call__(self, ctx: JobContext) -> JobStatusInfo:
        status = cast(
            JobStatusInfo,
            GetStatus(JobStatusRequestType.ALL, constants.STATUS_TIMEOUT, None)(ctx),
        )
        logger.info("Initial status: %s", pprint.pformat(status, sort_dicts=False))

        start = perf_counter()
        logger.info("Waiting on completion.", ctx=ctx)
        with ctx.connection.cursor() as cursor:
            job_state = cursor.var(str)
            cursor.callproc(
                name="DBMS_DATAPUMP.WAIT_FOR_JOB",
                keyword_parameters={
                    "handle": int(ctx.job_handle),
                    "job_state": job_state,
                },
            )
            job_state = cast(str, job_state.getvalue())

        end = perf_counter()
        logger.info(
            "Job completed with end state: %s (took: %.6f)",
            job_state,
            end - start,
            ctx=ctx,
        )
        status.job_state = JobState[job_state]
        return status


class AddFiles:
    def __init__(self, files: Iterable[OracleFile]):
        self.files = files
        self.overwrite = False

    @get_status_on_exception
    def __call__(self, ctx: OpenContext) -> None:
        logger.info(
            "Adding files: %s overwrite=%s", self.files, self.overwrite, ctx=ctx
        )
        for file in self.files:
            logger.info("Adding file %s", file, ctx=ctx)

            with ctx.connection.cursor() as cursor:
                cursor.callproc(
                    "DBMS_DATAPUMP.ADD_FILE",
                    keyword_parameters={
                        "handle": int(ctx.job_handle),
                        "filename": file.name,
                        "directory": file.directory.name,
                        "filetype": int(file.file_type),
                        "reusefile": int(self.overwrite),
                    },
                )


class ApplyDirectives:
    def __init__(
        self,
        directives: Iterable[DirectiveBase],
        stage: Stage,
    ):
        self.directives = directives
        self.stage = stage

    @get_status_on_exception
    def __call__(self, ctx: OpenContext) -> None:
        logger.info("Applying %s directives.", self.stage, ctx=ctx)

        directives = (d for d in self.directives if d.stage is self.stage)
        for directive in directives:
            logger.info("Applying directive: %s", repr(directive), ctx=ctx)
            directive.apply(ctx)


class LogEntry:
    def __init__(self, msg: str):
        self.msg = msg

    def __call__(self, ctx: OpenContext) -> None:
        logger.debug("Writing datapump log entry.", ctx=ctx)

        with ctx.connection.cursor() as cursor:
            cursor.callproc(
                name="DBMS_DATAPUMP.LOG_ENTRY",
                keyword_parameters={
                    "handle": int(ctx.job_handle),
                    "message": str(self.msg),
                },
            )


class WriteMetaData:
    def __init__(self, file: OracleFile, metadata: JobMetaData):
        self.file = file
        self.metadata = metadata
        self.file_content = json.dumps(metadata, cls=JsonSerializer, indent=4)

    def __call__(self, ctx: OpenContext) -> None:
        LogEntry(f"job metadata: {self.file.name}")(ctx)

        with ora_open(self.file, "w") as f:
            f.write(self.file_content)


def job_exists(job_name: str, job_owner: str, connection: Connection) -> bool:
    logger.debug(
        "Checking for job existence: job_name=%s job_owner=%s", job_name, job_owner
    )
    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL_GET_DATAPUMP_JOB,
            parameters={"job_owner": job_owner, "job_name": job_name},
        )
        result = next(iter(cursor.fetchall()), None)
        logger.debug("Job existence result: %s", result)

        return result is not None
