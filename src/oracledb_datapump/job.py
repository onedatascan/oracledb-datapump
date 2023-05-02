import getpass
from datetime import datetime, timezone
from functools import cached_property
from time import sleep
from typing import Iterable, cast

from oracledb_datapump import constants
from oracledb_datapump.base import ConnectDict, JobMetaData, JobMode, Operation, Stage
from oracledb_datapump.commands import (
    AddFiles,
    ApplyDirectives,
    Detach,
    GetStatus,
    Start,
    WaitOnCompletion,
    WriteMetaData,
)
from oracledb_datapump.database import (
    Connection,
    Schema,
    dt_to_scn,
    get_connection,
    to_db_timezone,
)
from oracledb_datapump.directives import DT, Directive, DirectiveBase, Extra
from oracledb_datapump.exceptions import JobNotFound, UsageError
from oracledb_datapump.files import (
    DumpFile,
    FileType,
    LogFile,
    OracleDirectory,
    OracleFile,
    ora_open,
)
from oracledb_datapump.handlers import (
    ContextHandler,
    FileHandler,
    attach_job,
    get_status_handler,
    open_job,
)
from oracledb_datapump.log import get_logger
from oracledb_datapump.status import JobState, JobStatusInfo, JobStatusRequestType
from oracledb_datapump.util import string_shortener

logger = get_logger(__name__)


class Job:
    def __init__(
        self,
        operation: Operation | None = None,
        mode: JobMode | None = None,
        dumpfiles: Iterable[str] | None = None,
        directives: Iterable[DirectiveBase] | None = None,
        connection: Connection | ConnectDict | str | None = None,
        tag: str | None = None,
        job_name: str | None = None,
        job_owner: str | None = None,
    ):
        self.operation = operation
        self.mode = mode

        if self.operation is Operation.IMPORT and not dumpfiles:
            raise UsageError("Dumpfiles argument required for import job")

        self._default_directory: str = constants.DEFAULT_DP_DIR
        self._file_uris = dumpfiles or []
        self.dumpfiles: set[DumpFile] = set()
        self.logfile: LogFile | None = None

        self.directives = list(directives) if directives is not None else []
        self._connection: Connection | None = (
            get_connection(connection) if connection else None
        )
        self.tag = tag
        self.metadata = {}
        self.metadata_file: OracleFile | None = None
        self._job_name = job_name
        self._job_owner = job_owner
        self._job_date = None
        self.datapump_handler = None

    @property
    def connection(self) -> Connection | None:
        return self._connection

    @property
    def job_name(self) -> str | None:
        if self._job_name:
            return self._job_name

        if not self.operation or not self.job_date:
            return None

        if self.operation == Operation.EXPORT and self.schemas:
            ident = constants.NAME_DELIM.join(s.name for s in self.schemas)
        elif self.operation == Operation.IMPORT and self.has_directive(
            Directive.REMAP_SCHEMA
        ):
            remaps = self.get_directive(Directive.REMAP_SCHEMA)
            first = next(iter(remaps))
            ident = constants.NAME_DELIM.join((first.old_value, "TO", first.value))
        else:
            ident = self.job_owner.upper() if self.job_owner else ""

        if self.tag:
            ident += (
                constants.NAME_DELIM
                + constants.TAG_DELIM
                + self.tag
                + constants.TAG_DELIM
            )

        # Datapump job_name has a max char limit of 128
        self._job_name = string_shortener(
            constants.NAME_DELIM.join(
                [
                    self.operation.abbrv(),
                    string_shortener(ident, 32),
                    self.job_date.strftime(constants.DATE_STR_FMT),
                ]
            ),
            128,
        )
        return self._job_name

    @property
    def job_owner(self) -> str | None:
        return self._job_owner

    @property
    def job_date(self) -> datetime | None:
        return self._job_date

    @property
    def parallel(self) -> int:
        for d in self.directives:
            if isinstance(d, Directive.PARALLEL):
                return int(cast(int, d.value))
        return 1

    @cached_property
    def schemas(self) -> list[Schema]:
        if not self.connection:
            raise UsageError("Database context not set!")

        schemas = []
        for i in self.directives:
            if isinstance(i, Directive.INCLUDE_SCHEMA):
                s = i.value
                if isinstance(s, Schema):
                    schema = s
                else:
                    schema = Schema(s)

                schema.get_handler(self.connection)
                schemas.append(schema)
        return schemas

    def _get_default_file_name(self, file_type: FileType) -> str:
        assert self.job_name
        return self.job_name + file_type.suffix

    def _build_metadata_file(self) -> OracleFile:
        assert self.connection
        assert self.metadata
        self.metadata = cast(JobMetaData, self.metadata)
        filename = self._get_default_file_name(FileType.META_FILE)
        directory_names: list[str] | None = self.metadata.get("directories")

        directory = None
        if directory_names:
            if self._default_directory in directory_names:
                directory = OracleDirectory(self._default_directory, self.connection)
            else:
                directory = OracleDirectory(directory_names[0], self.connection)

        return OracleFile(filename, self.connection, directory)

    def has_directive(self, directive: type[DirectiveBase]) -> bool:
        return any(isinstance(i, directive) for i in self.directives)

    def get_directive(self, directive: type[DT]) -> list[DT]:
        return [i for i in self.directives if isinstance(i, directive)]

    def _add_metadata(self, metadata: dict) -> None:
        for k, new_val in metadata.items():
            if k in self.metadata:
                if isinstance(self.metadata[k], list):
                    if isinstance(new_val, list):
                        self.metadata[k].extend(new_val)
                    else:
                        self.metadata[k].append(new_val)
                else:
                    cur_val = self.metadata[k]
                    self.metadata[k] = [cur_val, new_val]
            else:
                self.metadata[k] = new_val

    def run(
        self,
        wait: bool = False,
        connection: Connection | ConnectDict | str | None = None,
    ) -> JobStatusInfo:
        if connection:
            self._connection = get_connection(connection)

        if not self.connection:
            raise UsageError("Database connection not set!")

        if not self.operation:
            raise UsageError("Datapump operation not set!")

        if not self.mode:
            raise UsageError("Datapump mode not set!")

        self.wait = wait
        self._job_date = datetime.now(timezone.utc)

        if not self._job_owner:
            self._job_owner = self.connection.username.upper()

        assert self.job_name
        assert self.job_owner
        assert self.job_date

        self.logfile = LogFile(
            self._get_default_file_name(FileType.LOG_FILE), self.connection
        )

        self.datapump_handler = open_job(
            self.connection,
            self.job_name,
            self.job_owner,
            self.operation,
            self.mode,
            self.parallel,
        )

        # TODO: Extract Job metadata object creation out to a separate builder class
        self._add_metadata(
            {
                "job_name": self.job_name,
                "job_owner": self.job_owner,
                "job_date": self.job_date,
                "executed_by": getpass.getuser(),
                "tag": self.tag,
                "operation": str(self.operation),
                "mode": str(self.mode),
                "schemas": [s.name for s in self.schemas],
                "schema_versions": {s.name: s.schema_version for s in self.schemas},
                "tablespaces": [t.name for s in self.schemas for t in s.tablespaces],
                "source_db": self.connection.dsn.split("/")[1],
                "source_host": self.connection.dsn.split("/")[0],
            }
        )

        file_handler = FileHandler(self.datapump_handler.ctx)

        for file in self._file_uris:
            file_handler.handle(file)

        self.dumpfiles = file_handler.produce()
        self._add_metadata({"dumpfiles": [str(f.path) for f in self.dumpfiles]})
        self._add_metadata(
            {"directories": list({f.directory.name for f in self.dumpfiles})}
        )

        if self.operation == Operation.EXPORT and not (
            self.has_directive(Directive.FLASHBACK_SCN)
            or self.has_directive(Directive.FLASHBACK_TIME)
        ):
            logger.debug("Fetching SCN as of %s", self.job_date.isoformat())
            export_scn = dt_to_scn(
                to_db_timezone(self.job_date, self.connection), self.connection
            )
            self.directives.append(Directive.FLASHBACK_SCN(export_scn))

        for directive in self.directives:
            logger.debug("Adding directive metadata for: %s", directive)
            self._add_metadata(directive.metadata)

        self.metadata_file = self._build_metadata_file()

        commands = [
            ApplyDirectives(self.directives, Stage.PRE),
            AddFiles([self.logfile]),
            WriteMetaData(self.metadata_file, cast(JobMetaData, self.metadata)),
            AddFiles(self.dumpfiles),
            ApplyDirectives(self.directives, Stage.DATAPUMP),
            Start(),
        ]
        for command in commands:
            self.datapump_handler.send(command)

        if self.wait:
            status = self.datapump_handler.send(WaitOnCompletion())

            extras = self.get_directive(Extra)
            for extra in extras:
                extra.job_metadata = cast(JobMetaData, self.metadata)

            self.datapump_handler.send(ApplyDirectives(self.directives, Stage.POST))

        else:
            status = self.datapump_handler.send(
                GetStatus(
                    JobStatusRequestType.ALL,
                    timeout=constants.STATUS_TIMEOUT,
                    logfile=None,
                )
            )

        self.datapump_handler.send(Detach())

        return status

    def get_status(
        self, status_type: JobStatusRequestType = JobStatusRequestType.LOG_STATUS
    ) -> JobStatusInfo | None:
        if not self.connection or not self.job_name or not self.job_owner:
            return None
        return get_info(self.connection, self.job_name, self.job_owner, status_type)

    def poll_for_completion(self, rate: int = 30) -> JobStatusInfo:
        if not self.connection or not self.job_name or not self.job_owner:
            raise UsageError("Run job before polling!")
        return poll_for_completion(self.connection, self.job_name, self.job_owner)

    def get_logfile(self) -> str:
        if not self.connection or not self.job_name or not self.job_owner:
            return ""
        print("logfile is:", self.logfile)
        assert self.logfile
        with ora_open(self.logfile, "r") as fh:
            return fh.read()

    @classmethod
    def attach(
        cls, connection: Connection | ConnectDict | str, job_name: str, job_owner: str
    ):
        job = cls(connection=connection, job_name=job_name, job_owner=job_owner)
        try:
            job.datapump_handler = attach(connection, job_name, job_owner)
        except JobNotFound as e:
            logger.warning(e)
            assert job.connection
            job.datapump_handler = get_status_handler(
                job.connection, job_name, job_owner
            )

        assert job.connection
        job.logfile = LogFile(
            job._get_default_file_name(FileType.LOG_FILE), job.connection
        )

        return job


def attach(
    connection: Connection | ConnectDict | str, job_name: str, job_owner: str
) -> ContextHandler:
    connection = get_connection(connection)
    return attach_job(connection, job_name, job_owner)


def get_status(
    connection: Connection | ConnectDict | str,
    job_name: str,
    job_owner: str,
    status_type: JobStatusRequestType | None = None,
) -> JobStatusInfo:
    connection = get_connection(connection)
    handler = get_status_handler(connection, job_name, job_owner)
    status = handler.send(GetStatus(status_type, timeout=-1, logfile=None))
    return status


def get_info(
    connection: Connection | ConnectDict | str,
    job_name: str,
    job_owner: str,
    status_type: JobStatusRequestType,
    timeout: int = -1,
) -> JobStatusInfo:
    connection = get_connection(connection)
    try:
        handler = attach(connection, job_name, job_owner)
        status = handler.send(GetStatus(status_type, timeout=timeout, logfile=None))
    except Exception:
        handler = get_status_handler(connection, job_name, job_owner)
        status = handler.send(
            GetStatus(JobStatusRequestType.LOG_STATUS, timeout=timeout, logfile=None)
        )
    return status


def poll_for_completion(
    connection: Connection | ConnectDict | str,
    job_name: str,
    job_owner: str,
    rate=30,
) -> JobStatusInfo:
    logger.info("Polling for completion. rate=%ss", rate)
    connection = get_connection(connection)
    response = get_status(connection, job_name, job_owner)
    logger.info(str(response))
    state = response.job_state
    while state not in (
        JobState.COMPLETED,
        JobState.COMPLETED_WITH_ERRORS,
        JobState.STOPPED,
    ):
        sleep(rate)
        response = get_status(connection, job_name, job_owner)
        logger.debug(str(response))
        state = response.job_state
        logger.info(str(state))
    return response
