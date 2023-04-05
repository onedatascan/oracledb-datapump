from __future__ import annotations

import abc
from datetime import datetime
from enum import Enum
from typing import Literal, NotRequired, TypeAlias, TypedDict

import pydantic

from oracledb_datapump import constants
from oracledb_datapump.base import ConnectDict, JobMode, Operation
from oracledb_datapump.database import Connection, get_connection
from oracledb_datapump.directives import DirectiveBase
from oracledb_datapump.files import DumpFile, LogFile, OracleFile
from oracledb_datapump.job import Job, get_info, get_status, poll_for_completion
from oracledb_datapump.log import get_logger
from oracledb_datapump.status import (
    DumpFileInfo,
    DumpFileInfoItem,
    DumpFileType,
    JobStatusInfo,
    JobStatusRequestType,
)

logger = get_logger(__name__)

JsonPrimitives: TypeAlias = int | bool | str
JsonStr: TypeAlias = str


class JobDirective(TypedDict):
    name: str
    value: JsonPrimitives
    old_value: NotRequired[str | None]  # Remap
    object_path: NotRequired[str | None]
    kwargs: NotRequired[dict[str, JsonPrimitives] | None]  # Extra


class JobDirectiveModel(pydantic.BaseModel):
    name: str
    value: JsonPrimitives
    old_value: str | None = None
    object_path: str | None = None
    kwargs: dict[str, JsonPrimitives] | None

    class Config:
        fields = {
            "old_value": {"exclude_unset": True, "exclude_none": True},
            "object_path": {"exclude_unset": True, "exclude_none": True},
            "kwargs": {"exclude_unset": True, "exclude_none": True},
        }


class ConnectModel(pydantic.BaseModel):
    user: str
    password: pydantic.SecretStr
    host: str
    database: str
    port: int = pydantic.Field(default=constants.DEFAULT_SQLNET_PORT)


class SubmitPayload(pydantic.BaseModel):
    operation: Literal["IMPORT", "EXPORT"]
    mode: Literal["FULL", "SCHEMA", "TABLE", "TABLESPACE"]
    wait: bool = False
    dumpfiles: list[str] | None = None  # Required for IMPORT
    directives: list[JobDirectiveModel] | None = None
    tag: str | None = None


class StatusPayload(pydantic.BaseModel):
    job_name: str
    job_owner: str
    type: Literal["ALL", "STATUS", "DESC", "ERROR", "LOG_STATUS"] = "LOG_STATUS"


class PollPayload(pydantic.BaseModel):
    job_name: str
    job_owner: str
    rate: int


class RequestBase(pydantic.BaseModel, abc.ABC):
    connection: ConnectModel
    request: Literal["SUBMIT", "STATUS", "POOL"]
    payload: SubmitPayload | StatusPayload | PollPayload

    class Config:
        arbitrary_types_allowed = True
        smart_union = True

    @pydantic.validator("payload")
    def check_consistency(cls, value, values):
        allowed = {
            "SUBMIT": SubmitPayload,
            "STATUS": StatusPayload,
            "POLL": PollPayload,
        }
        if "request" in values and not isinstance(value, allowed[values["request"]]):
            raise ValueError(f"Wrong payload type for {values['request']}")
        return value


class SubmitRequest(RequestBase):
    request: Literal["SUBMIT"]
    payload: SubmitPayload


class StatusRequest(RequestBase):
    request: Literal["STATUS"]
    payload: StatusPayload


class PollRequest(RequestBase):
    request: Literal["POLL"]
    payload: PollPayload


Request = SubmitRequest | StatusRequest | PollRequest


class RequestHandler:
    handlers = {}

    def __init_subclass__(cls, request_type: type[object], **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        cls.handlers[request_type] = cls

    def handle(self, request: Request) -> Response:
        request = pydantic.parse_obj_as(Request, request)
        logger.debug("Received request type: %s", type(request))
        logger.info("Received request: %s", request)

        return self.handlers[type(request)].handle(request)

    @classmethod
    def build_status_response(
        cls, job_name: str, job_owner: str, status: JobStatusInfo
    ) -> Response:
        state = status.job_state.value if status.job_state else None
        logfile_exists = not any("logfile not found" in e for e in status.exception)
        logfile = str(status.logfile.path) if status.logfile else None
        dumpfiles = (
            [str(df.path) for df in status.dumpfiles] if status.dumpfiles else []
        )

        return Response(
            job_name=job_name,
            job_owner=job_owner,
            state=state,
            info=status,
            logfile=logfile,
            logfile_exists=logfile_exists,
            dumpfiles=dumpfiles,
        )

    @classmethod
    def build_connect_dict(cls, connection: ConnectModel):
        return ConnectDict(
            user=connection.user,
            password=connection.password.get_secret_value(),
            host=connection.host,
            database=connection.database,
            port=connection.port,
        )


class JobRequestHandler(RequestHandler, request_type=SubmitRequest):
    @classmethod
    def handle(cls, request: SubmitRequest) -> Response:
        logger.debug("Handling %s", type(request))

        connection = request.connection

        if not isinstance(connection, Connection):
            connection = get_connection(cls.build_connect_dict(request.connection))

        payload_directives = request.payload.directives or []
        directives = []

        for d in payload_directives:
            logger.debug("Parsing directive %s", d)
            mapping = d.dict(exclude_none=True)
            dtype = DirectiveBase.registry[mapping.pop("name")]  # type: ignore
            directive = dtype(**mapping)
            logger.debug("Parsed directive: %s", directive)
            directives.append(directive)

        job = Job(
            operation=Operation(request.payload.operation),
            mode=JobMode(request.payload.mode),
            dumpfiles=request.payload.dumpfiles,
            directives=directives,
            connection=connection,
            tag=request.payload.tag,
        )
        status: JobStatusInfo = job.run(wait=request.payload.wait)
        assert job.job_name
        assert job.job_owner

        return cls.build_status_response(job.job_name, job.job_owner, status)


class JobStatusHandler(RequestHandler, request_type=StatusRequest):
    @classmethod
    def handle(cls, request: StatusRequest) -> Response:
        logger.debug("Handling %s", type(request))

        connection = request.connection

        if not isinstance(connection, Connection):
            connection = get_connection(cls.build_connect_dict(request.connection))

        if request.payload.type == "LOG_STATUS":
            status = get_status(
                connection=connection,
                job_name=request.payload.job_name,
                job_owner=request.payload.job_owner,
            )
        else:
            status = get_info(
                connection=connection,
                job_name=request.payload.job_name,
                job_owner=request.payload.job_owner,
                status_type=JobStatusRequestType[request.payload.type],
            )

        return cls.build_status_response(
            request.payload.job_name, request.payload.job_owner, status
        )


class PollRequestHandler(RequestHandler, request_type=PollRequest):
    @classmethod
    def handle(cls, request: PollRequest) -> Response:
        logger.debug("Handling %s", type(request))

        connection = request.connection

        if not isinstance(connection, Connection):
            connection = get_connection(cls.build_connect_dict(request.connection))

        status = poll_for_completion(
            connection=connection,
            job_name=request.payload.job_name,
            job_owner=request.payload.job_owner,
            rate=request.payload.rate,
        )
        return cls.build_status_response(
            request.payload.job_name, request.payload.job_owner, status
        )


class Response(pydantic.BaseModel):
    job_name: str
    job_owner: str
    state: Literal[
        "DEFINING",
        "EXECUTING",
        "COMPLETING",
        "COMPLETED",
        "STOP_PENDING",
        "STOPPING",
        "STOPPED",
        "IDLING",
        "NOT_RUNNING",
        "ERROR",
    ] | None
    info: JobStatusInfo
    logfile: str | None
    logfile_exists: bool
    dumpfiles: list[str]

    class Config:
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            bytes: lambda v: v.hex(),
            Enum: lambda v: str(v.name),
            LogFile: lambda v: str(v.path),
            DumpFile: lambda v: str(v.path),
            OracleFile: lambda v: str(v.path),
        }


def dumpfile_info_builder(info: list[dict], file_type: int):
    k = (DumpFileInfoItem(int(i["item_code"])).name.lower() for i in info)
    v = (i.get("value") for i in info)
    return DumpFileInfo(file_type=DumpFileType(file_type), **dict(zip(k, v)))
