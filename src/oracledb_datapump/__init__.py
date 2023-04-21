from oracledb_datapump.base import ConnectDict, JobMode, Operation
from oracledb_datapump.database import Connection, Schema
from oracledb_datapump.directives import (
    Directive,
    Extra,
    Filter,
    Parameter,
    Remap,
    Transform,
)
from oracledb_datapump.exceptions import BadRequest
from oracledb_datapump.job import Job
from oracledb_datapump.request import (
    JobDirective,
    Response,
    StatusPayload,
    SubmitPayload,
)

__all__ = [
    "ConnectDict",
    "SubmitPayload",
    "StatusPayload",
    "Response",
    "JobDirective",
    "Connection",
    "Schema",
    "BadRequest",
    "JobMode",
    "Operation",
    "Directive",
    "Filter",
    "Parameter",
    "Remap",
    "Transform",
    "Extra",
    "Job",
]


def lambda_handler(event, context):
    from oracledb_datapump.entrypoints.aws_lambda import lambda_handler as handler

    return handler(event, context)
