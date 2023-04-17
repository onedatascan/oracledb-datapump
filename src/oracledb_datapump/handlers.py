from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Generic, Protocol, TypeVar
from urllib.parse import urlparse

from oracledb_datapump.base import JobMode, Operation
from oracledb_datapump.commands import attach_datapump, open_datapump
from oracledb_datapump.context import CTX, AttachContext, OpenContext, StatusContext
from oracledb_datapump.database import Connection
from oracledb_datapump.exceptions import UsageError
from oracledb_datapump.files import DumpFile, DumpFileSet
from oracledb_datapump.log import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


class ContextHandlerInitializer(Protocol[CTX]):
    def __call__(self, *args, **kwargs) -> CTX:
        ...


class ContextHandler(Generic[CTX]):
    """
    Handles partials/callables that only need a final
    context argument.
    """

    def __init__(self, ctx: CTX):
        self.ctx = ctx

    def send(self, task: Callable[[CTX], T]) -> T:
        return task(self.ctx)


# Context handler factory functions


def open_job(
    database_ctx: Connection,
    job_name: str,
    job_owner: str,
    operation: Operation,
    mode: JobMode,
    parallel: int,
) -> ContextHandler[OpenContext]:

    job_handle = open_datapump(job_name, operation, mode, parallel, database_ctx)

    return ContextHandler(
        OpenContext(
            connection=database_ctx,
            job_name=job_name,
            job_handle=job_handle,
            job_owner=job_owner,
            operation=operation,
            mode=mode,
            parallel=parallel,
        )
    )


def attach_job(
    database_ctx: Connection,
    job_name: str,
    job_owner: str,
) -> ContextHandler[AttachContext]:

    job_handle = attach_datapump(job_name, job_owner, database_ctx)

    return ContextHandler(
        AttachContext(
            connection=database_ctx,
            job_name=job_name,
            job_owner=job_owner,
            job_handle=job_handle,
        )
    )


def get_status_handler(
    database_ctx: Connection,
    job_name: str,
    job_owner: str,
) -> ContextHandler[StatusContext]:
    return ContextHandler(
        StatusContext(
            connection=database_ctx,
            job_name=job_name,
            job_owner=job_owner,
        )
    )


# File Handlers


def is_uri(s: str) -> bool:
    return urlparse(s).scheme is not None


@dataclass
class FileUri:
    scheme: str
    username: str | None
    password: str | None
    host: str | None
    port: int | None
    path: str | None


class FileHandler:
    handlers = {}
    _dumpfiles = []
    _pending_property_file = None

    def __init__(self, ctx: OpenContext):
        self.ctx = ctx

    def __init_subclass__(cls, file_type: type[object], **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        logger.debug("Initializing FileHandler type: %s", file_type)
        cls.handlers[file_type] = cls

    def handle(self, file: str) -> None:
        # TODO: implement URI parsing to allow pluggable file handlers for
        # various URI schemes.
        if is_uri(file):
            handler = self.handlers[str]
        else:
            handler = self.handlers[str]
        logger.debug("Handling dumpfile arg: %s", file)
        handler.handle(file, self.ctx)

    def produce(self) -> set[DumpFile]:
        if self.ctx.operation is Operation.IMPORT and self._dumpfiles is None:
            raise UsageError("Dumpfiles argument required for import job")

        logger.debug("Producing dumpfile set from args: %s", self._dumpfiles)
        dumpfile_set = DumpFileSet(*self._dumpfiles)
        dumpfile_set.prepare(
            connection=self.ctx.connection,
            file_set_ident=self.ctx.job_name,
            operation=self.ctx.operation,
            parallel=self.ctx.parallel,
        )

        return dumpfile_set.dumpfiles


class NoSchemeHandler(FileHandler, file_type=str):
    @classmethod
    def handle(cls, file: str, ctx: OpenContext) -> None:
        cls._dumpfiles.append(file)
