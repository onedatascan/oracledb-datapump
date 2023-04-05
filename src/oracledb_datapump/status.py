from __future__ import annotations

import functools
import itertools
import re
from datetime import datetime
from enum import Enum, Flag, IntEnum
from pathlib import PurePath
from typing import Callable, ParamSpec, Self, TypeVar, cast

import pydantic

from oracledb_datapump import constants, sql
from oracledb_datapump.base import JobMode, Operation
from oracledb_datapump.context import JobContext, StatusContext
from oracledb_datapump.database import DB_OBJECT_TYPE, Connection
from oracledb_datapump.exceptions import (
    BadRequest,
    DatabaseError,
    DataPumpCompletedWithErrors,
    UsageError,
)
from oracledb_datapump.files import (
    DumpFile,
    FileType,
    LogFile,
    OracleDirectory,
    OracleFile,
    ora_open,
)
from oracledb_datapump.log import get_logger
from oracledb_datapump.util import object_loads

logger = get_logger(__name__)


class JobState(Enum):
    UNDEFINED = None
    DEFINING = "DEFINING"
    EXECUTING = "EXECUTING"
    COMPLETING = "COMPLETING"
    COMPLETED = "COMPLETED"
    STOP_PENDING = "STOP_PENDING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    IDLING = "IDLING"
    NOT_RUNNING = "NOT_RUNNING"
    ERROR = "ERROR"

    def __str__(self):
        return str(self.value)


class FloatConvertingIntEnum(IntEnum):
    @classmethod
    def _missing_(cls, value) -> Self | None:
        if isinstance(value, float):
            value = int(value)
            for member in cls:
                if member.value == value:
                    return member
        return None


class DumpFileEncryptMode(FloatConvertingIntEnum):
    UNKNOWN = 1
    NONE = 2
    PASSWORD = 3
    DUAL = 4
    WALLET = 5


class DumpFileCompressionAlgo(FloatConvertingIntEnum):
    UNKNOWN = 1
    NONE = 2
    BASIC = 3
    LOW = 4
    MEDIUM = 5
    HIGH = 6


class DumpFileInfoItem(FloatConvertingIntEnum):
    UNKNOWN = 0
    FILE_VERSION = 1
    MASTER_PRESENT = 2
    JOB_GUID = 3
    FILE_NUM = 4
    CHARSET_ID = 5
    CREATE_DATE = 6
    FLAGS = 7
    JOB_NAME = 8
    PLATFORM = 9
    SOURCE_DB = 10
    NLS_CHARACTERSET = 11
    BLOCKSIZE = 12
    DIRECT_PATH = 13
    META_COMPRESSED = 14
    DB_VERSION = 15
    MASTER_PIECE_CNT = 16
    MASTER_PIECE_NUM = 17
    DATA_COMPRESSED = 18
    META_ENCRYPTED = 19
    DATA_ENCRYPTED = 20
    COLS_ENCRYPTED = 21
    ENCRYPT_MODE = 22
    COMPRESSION_ALGO = 23
    HADOOP_TRAILERS = 24
    MAX_ITEM_CODE = 25


class DumpFileType(FloatConvertingIntEnum):
    UNKNOWN = 0
    DATAPUMP = 1
    LEGACY = 2
    EXTERNAL_TABLE = 3


class StatusBase(pydantic.BaseModel):
    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            bytes: lambda v: v.hex(),
            Enum: lambda v: str(v.name),
            LogFile: lambda v: str(v.path),
            DumpFile: lambda v: str(v.path),
            OracleFile: lambda v: str(v.path),
        }


class DumpFileInfo(StatusBase):
    file_type: DumpFileType | None = None
    file_version: float | None = None
    master_present: bool | None = None
    job_guid: str | None = None
    file_num: int | None = None
    charset_id: str | None = None
    create_date: datetime | None = None
    flags: int | None = None
    job_name: str | None = None
    platform: str | None = None
    source_db: str | None = None
    nls_characterset: str | None = None
    blocksize: int | None = None
    direct_path: bool | None = None
    meta_compressed: bool | None = None
    db_version: str | None = None
    master_piece_cnt: int | None = None
    master_piece_num: int | None = None
    data_compressed: bool | None = None
    meta_encrypted: bool | None = None
    data_encrypted: bool | None = None
    cols_encrypted: bool | None = None
    encrypt_mode: DumpFileEncryptMode | None = None
    compression_algo: DumpFileCompressionAlgo | None = None
    hadoop_trailers: int | None = None
    max_item_code: int | None = None

    @pydantic.validator("create_date", pre=True, always=False)
    def parse_create_dt(cls, value) -> datetime:
        if isinstance(value, datetime):
            return value
        return datetime.strptime(value, constants.DMP_CREATE_DATE_STR_FMT)


class JobLogEntry(StatusBase):
    loglinenumber: int | None = pydantic.Field(alias="log_line_num")
    logtext: str | None = pydantic.Field(alias="log_text")
    errornumber: int | None = pydantic.Field(alias="error_num")

    class Config(StatusBase.Config):
        allow_population_by_field_name = True


class JobParam(StatusBase):
    param_name: str | None = None
    param_op: str | None = None
    param_type: str | None = None
    param_length: int | None = None
    param_value_n: int | None = None
    param_value_t: str | None = None

    @property
    def param_value(self) -> str | int | None:
        return self.param_value_n if self.param_value_n else self.param_value_t


class JobDescription(StatusBase):
    job_name: str | None = None
    guid: str | None = None
    operation: Operation | None = None
    job_mode: JobMode | None = None
    remote_link: str | None = None
    owner: str | None = None
    platform: str | None = None
    exp_platform: str | None = None
    global_name: str | None = None
    exp_global_name: str | None = None
    instance: str | None = None
    db_version: str | None = None
    exp_db_version: str | None = None
    job_version: str | None = None
    scn: int | None = None
    creator_privs: str | None = None
    start_time: datetime | None = None
    exp_start_time: datetime | None = None
    term_reason: int | None = None
    max_degree: int | None = None
    timezone: str | None = None
    exp_timezone: str | None = None
    tstz_version: int | None = None
    exp_tstz_version: int | None = None
    endianness: str | None = None
    exp_endianness: str | None = None
    charset: str | None = None
    exp_charset: str | None = None
    ncharset: str | None = None
    exp_ncharset: str | None = None
    log_file: str | None = None
    sql_file: str | None = None
    params: list[JobParam] | None = None

    @pydantic.validator("guid", pre=True)
    def guidtohex(cls, value):
        if isinstance(value, bytes):
            return value.hex()
        return value


class JobWorkerStatus(StatusBase):
    worker_number: int | None = None
    process_name: str | None = None
    state: str | None = None
    schema_: str | None = pydantic.Field(default=None, alias="schema")
    name: str | None = None
    object_type: str | None = None
    partition: str | None = None
    completed_objects: int | None = None
    total_objects: int | None = None
    completed_rows: int | None = None
    completed_bytes: int | None = None
    percent_done: float | None = None
    degree: int | None = None
    instance_id: int | None = None
    instance_name: str | None = None
    host_name: str | None = None
    access_method: str | None = None
    obj_start_time: datetime | None = None
    obj_status: datetime | None = None


class JobDumpFile(StatusBase):
    file_name: str
    file_type: int
    file_size: int
    file_bytes_written: int

    @property
    def path(self) -> PurePath:
        return PurePath(self.file_name)


class JobStatus(StatusBase):
    job_name: str
    operation: Operation
    job_mode: JobMode
    bytes_processed: int
    total_bytes: int
    percent_done: float
    degree: int
    error_count: int
    state: str
    phase: int
    restart_count: int
    heartbeat: int
    worker_status_list: list[JobWorkerStatus]
    files: list[JobDumpFile]


class JobStatusMask(Flag):
    WIP = 1
    DESC = 2
    STATUS = 4
    ERROR = 8


class JobStatusRequestType(Enum):
    ALL = (
        JobStatusMask.WIP
        | JobStatusMask.STATUS
        | JobStatusMask.ERROR
        | JobStatusMask.DESC
    ).value
    STATUS = (JobStatusMask.WIP | JobStatusMask.STATUS | JobStatusMask.ERROR).value
    DESC = JobStatusMask.DESC.value
    ERROR = JobStatusMask.ERROR.value
    LOG_STATUS = 0


class JobStatusInfo(StatusBase):
    job_state: JobState | None = None
    mask: JobStatusMask | None = None
    wip: list[JobLogEntry] = pydantic.Field(default_factory=list)
    job_description: JobDescription | None = None
    job_status: JobStatus | None = None
    error: list[JobLogEntry] = pydantic.Field(default_factory=list)
    exception: list[str] = pydantic.Field(default_factory=list)
    logfile: LogFile | None = None
    log_contents: list[str] = pydantic.Field(default_factory=list)
    dumpfiles: list[DumpFile] = pydantic.Field(default_factory=list)

    @pydantic.validator("mask", pre=True, always=False)
    def to_enum(cls, v) -> JobStatusMask | None:
        if v:
            return JobStatusMask(int(v))
        else:
            return None

    def add_exception(self, exception: Exception) -> None:
        if self.exception:
            self.exception.append(str(exception))
        else:
            self.exception = [str(exception)]

    def populate_logfile(self, connection: Connection) -> None:
        if self.job_description:
            if self.job_description.log_file:
                self.logfile = LogFile(self.job_description.log_file, connection)

    def populate_dumpfiles(self, connection: Connection) -> None:
        if self.job_status:
            if self.job_status.files:
                dumpfiles = [
                    DumpFile(f.path, connection) for f in self.job_status.files
                ]
                self.dumpfiles = dumpfiles


def get_job_status(
    ctx: JobContext | StatusContext,
    request_type: JobStatusRequestType,
    timeout: int = -1,
    logfile: LogFile | str | None = None,
) -> JobStatusInfo:
    if request_type is JobStatusRequestType.LOG_STATUS:
        return _build_log_job_status(ctx, logfile)
    else:
        if isinstance(ctx, JobContext):
            return _build_api_job_status(ctx, request_type, timeout)
        else:
            raise UsageError(
                "StatusContext cannot fetch Datapump API job status! Attach to active",
                " job to get running job status or request LOG_STATUS only.",
            )


def _build_api_job_status(
    ctx: JobContext, request_type: JobStatusRequestType, timeout: int = -1
) -> JobStatusInfo:
    """
    Fetches Datapump job status from the database using Datapump API. Various levels
    of detail can be fetched depending upon the request_type param with more detail
    taking longer to fetch. The datapump status API seems to get progressively slower
    and slower as the job progresses into the later stages to where it almost becomes
    unusable. Another drawback of this API is that once a job is complete it ceases
    to exist and so you cannot get the final outcome of the job from it. To poll for a
    simple a status and for a completion outcome its preferable to use the alternative
    function that parses the Datapump log file for the job.
    """

    logger.debug(
        "Fetching job status %s from Datapump API. timeout=%d",
        request_type,
        timeout,
        ctx=ctx,
    )
    status_obj_type: DB_OBJECT_TYPE = ctx.connection.gettype("KU$_STATUS")
    with ctx.connection.cursor() as cursor:
        status_obj_var = cursor.var(status_obj_type)
        job_state_var = cursor.var(str)
        cursor.callproc(
            name="DBMS_DATAPUMP.GET_STATUS",
            keyword_parameters={
                "handle": int(ctx.job_handle),
                "mask": int(request_type.value),
                "timeout": timeout,
                "job_state": job_state_var,
                "status": status_obj_var,
            },
        )
        status_obj = object_loads(status_obj_var.getvalue())  # type: ignore
        logger.debug("status object: %s", status_obj)
        assert isinstance(status_obj, dict)

        job_state = cast(str, job_state_var.getvalue())
        logger.debug("job state: %s", job_state)

    js = JobStatusInfo(job_state=JobState[job_state], **status_obj)
    js.populate_logfile(ctx.connection)
    js.populate_dumpfiles(ctx.connection)

    return js


def _build_log_job_status(
    ctx: JobContext | StatusContext, logfile: LogFile | str | None = None
) -> JobStatusInfo:
    """
    Fetches job status from Datapump logfile. Requires that the jobs Datapump
    logfile is on the database server in the default Oracle directory and
    assumes the default logfile name convention.
    """

    JOB_LOG_STATUS_RE: re.Pattern = re.compile(
        rf'Job \"{ctx.job_owner}\"\.\"{ctx.job_name}"\s(\w+\s\w+)\s(\d*\serror\(s\))?'
    )

    if not logfile:
        logfile = LogFile(
            file=ctx.job_name + FileType.LOG_FILE.suffix,
            connection=ctx.connection,
            directory=OracleDirectory.default(ctx.connection),
        )
    elif isinstance(logfile, str):
        logfile = LogFile(
            file=logfile,
            connection=ctx.connection,
        )
    else:
        raise UsageError(
            "Unrecognized logfile parameter type %s, %s",
            logfile,
            type(logfile),
        )

    if not logfile.exists:
        return JobStatusInfo(
            job_state=JobState.UNDEFINED,
            exception=[
                str(FileNotFoundError(f"logfile not found: {str(logfile.path)}"))
            ],
            logfile=logfile,
            log_contents=[],
        )
    elif logfile.size == 0:
        return JobStatusInfo(
            job_state=JobState.UNDEFINED, logfile=logfile, log_contents=[]
        )
    else:
        logger.debug("Found logfile: %s size: %s", str(logfile.path), logfile.size)

    logger.debug("Reading logfile: %s", str(logfile.path))
    with ora_open(logfile, "r") as lf:
        log_contents = lf.read().splitlines()

    logger.debug("Locating dumpfiles in logfile: %s", str(logfile.path))
    dumpfiles = []
    try:
        df_file_start_str = f"Dump file set for {ctx.job_owner}.{ctx.job_name} is:"
        df_start_idx = log_contents.index(df_file_start_str)
        dumpfiles = [
            DumpFile(line.strip(), ctx.connection)
            for line in log_contents[df_start_idx + 1:]
            if line.endswith(".dmp")
        ]
    except ValueError:
        logger.debug("Dumpfiles not found.")

    logger.debug("job_name: %s log file contents: %s", ctx.job_name, log_contents)
    last_line = log_contents[-1]
    logger.debug("Job log last line: %s", last_line)

    if found := JOB_LOG_STATUS_RE.search(last_line):
        logger.debug("job_name: %s log pattern search result: %s", ctx.job_name, found)
        result = found.groups()[0].strip()
        errors = found.groups()[1]
    else:
        result = None
        errors = None

    logger.debug("Job log result: %s", result)

    if result == "successfully completed":
        return JobStatusInfo(
            job_state=JobState.COMPLETED,
            logfile=logfile,
            log_contents=log_contents,
            dumpfiles=dumpfiles
        )
    elif result == "completed with" and errors:
        js = JobStatusInfo(
            job_state=JobState.COMPLETED,
            logfile=logfile,
            log_contents=log_contents,
            dumpfiles=dumpfiles,
        )
        js.add_exception(DataPumpCompletedWithErrors(errors))
        return js
    else:
        return JobStatusInfo(
            job_state=JobState.UNDEFINED,
            logfile=logfile,
            log_contents=log_contents,
            dumpfiles=dumpfiles,
        )


T = TypeVar("T")
P = ParamSpec("P")


def get_status_on_exception(fn: Callable[P, T]) -> Callable[P, T]:
    @functools.wraps(fn)
    def decorator(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            result = fn(*args, **kwargs)
        except DatabaseError as e:
            (error_obj,) = e.args
            logger.exception(e)
            logger.debug("Fetching status after exception on %s", fn.__qualname__)
            logger.debug("Error code: %s", error_obj.code)

            if str(error_obj.code) in (
                sql.SUCCESS_WITH_INFO,
                sql.INTERNAL_ERROR,
                sql.INVALID_OPERATION,
                sql.INVALID_ARGUMENT,
            ):
                logger.debug("Caller args: %s %s", args, kwargs)
                ctx = None
                for arg in itertools.chain(args, kwargs.values()):
                    if isinstance(arg, JobContext):
                        ctx = arg
                    if ctx:
                        try:
                            status = get_job_status(
                                ctx, JobStatusRequestType.ERROR, timeout=30
                            )
                        except Exception:
                            status = get_job_status(
                                ctx, JobStatusRequestType.LOG_STATUS, timeout=30
                            )
                        raise BadRequest(status) from e

                logger.exception(
                    "Could not find job or database context args in function: %s %s",
                    fn.__qualname__,
                    fn.__annotations__,
                )

            raise e
        else:
            return result

    return decorator
