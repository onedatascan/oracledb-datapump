from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Generic, Literal, NotRequired, TypeAlias, TypeVar, TypedDict

import pydantic
import pydantic.generics

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
    username: str
    password: pydantic.SecretStr
    hostname: str
    database: str
    port: int = pydantic.Field(default=constants.DEFAULT_SQLNET_PORT)


class Payload(pydantic.BaseModel):
    class Config:
        extra = pydantic.Extra.forbid


class SubmitPayload(Payload):
    operation: Literal["IMPORT", "EXPORT"]
    mode: Literal["FULL", "SCHEMA", "TABLE", "TABLESPACE"]
    wait: bool = False
    dumpfiles: list[str] | None = None  # Required for IMPORT
    directives: list[JobDirectiveModel] | None = None
    tag: str | None = None


class StatusPayload(Payload):
    job_name: str
    job_owner: str
    type: Literal["ALL", "STATUS", "DESC", "ERROR", "LOG_STATUS"] | None = None
    include_detail: bool = True


class PollPayload(Payload):
    job_name: str
    job_owner: str
    rate: int


# Cant use the superclass here because Pydantic seems unable to figure out
# which subclass to parse the data as.
PayloadT = TypeVar("PayloadT", SubmitPayload, StatusPayload, PollPayload)
RequestT = Literal["SUBMIT", "STATUS", "POLL"]


class Request(pydantic.generics.GenericModel, Generic[PayloadT]):
    connection: ConnectModel
    request: RequestT
    payload: PayloadT

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


class RequestHandler:
    handlers = {}

    def __init_subclass__(cls, request_type: RequestT, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        cls.handlers[request_type] = cls

    def handle(self, request: Request) -> Response:
        request = pydantic.parse_obj_as(Request, request)
        logger.debug("Received request type: %s", type(request))
        logger.info("Received request: %s", request)

        return self.handlers[request.request].handle(request)

    @classmethod
    def build_status_response(
        cls, job_name: str, job_owner: str, include_detail: bool, status: JobStatusInfo
    ) -> Response:
        state = status.job_state.value if status.job_state else None

        return Response(
            job_name=job_name,
            job_owner=job_owner,
            state=state,
            detail=status if include_detail else None,
        )

    @classmethod
    def build_connect_dict(cls, connection: ConnectModel):
        return ConnectDict(
            username=connection.username,
            password=connection.password.get_secret_value(),
            hostname=connection.hostname,
            database=connection.database,
            port=connection.port,
        )


class JobRequestHandler(RequestHandler, request_type="SUBMIT"):
    @classmethod
    def handle(cls, request: Request) -> Response:
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

        return cls.build_status_response(job.job_name, job.job_owner, True, status)


class JobStatusHandler(RequestHandler, request_type="STATUS"):
    @classmethod
    def handle(cls, request: Request) -> Response:
        logger.debug("Handling %s", type(request))

        connection = request.connection

        if not isinstance(connection, Connection):
            connection = get_connection(cls.build_connect_dict(request.connection))

        status_type = (
            JobStatusRequestType[request.payload.type] if request.payload.type else None
        )

        logger.debug("Status type is: %s", repr(status_type))

        if status_type is JobStatusRequestType.LOG_STATUS or status_type is None:
            status = get_status(
                connection=connection,
                job_name=request.payload.job_name,
                job_owner=request.payload.job_owner,
                status_type=status_type,
            )
        else:
            status = get_info(
                connection=connection,
                job_name=request.payload.job_name,
                job_owner=request.payload.job_owner,
                status_type=status_type,
            )

        return cls.build_status_response(
            request.payload.job_name,
            request.payload.job_owner,
            request.payload.include_detail,
            status,
        )


class PollRequestHandler(RequestHandler, request_type="POLL"):
    @classmethod
    def handle(cls, request: Request) -> Response:
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
            request.payload.job_name, request.payload.job_owner, True, status
        )


class Response(pydantic.BaseModel):
    job_name: str
    job_owner: str
    state: Literal[
        "DEFINING",
        "EXECUTING",
        "COMPLETING",
        "COMPLETED",
        "COMPLETED_WITH_ERRORS",
        "STOP_PENDING",
        "STOPPING",
        "STOPPED",
        "IDLING",
        "NOT_RUNNING",
        "ERROR",
    ] | None
    detail: JobStatusInfo | None

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
