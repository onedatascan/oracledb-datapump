import base64
import json
import os
from collections import defaultdict
from http import HTTPStatus
from typing import Final, Protocol, TypeAlias, TypedDict, cast, runtime_checkable

from aws_lambda_powertools import Logger
from aws_lambda_powertools.logging.utils import copy_config_to_registered_loggers
from aws_lambda_powertools.utilities import parameters
from aws_lambda_powertools.utilities.parser import (
    BaseModel,
    ValidationError,
    event_parser,
    models,
    parse,
    root_validator,
)
from aws_lambda_powertools.utilities.parser.pydantic import Extra, Json, parse_obj_as
from aws_lambda_powertools.utilities.typing import LambdaContext

from oracledb_datapump.client import DataPump
from oracledb_datapump.constants import SERVICE_NAME
from oracledb_datapump.request import ConnectModel, Request

logger = Logger(service=SERVICE_NAME, level=os.getenv("LOG_LEVEL", "INFO"))
copy_config_to_registered_loggers(logger)

ENVELOPE: Final[str | None] = os.getenv("ENVELOPE")

json_types: TypeAlias = str | int | dict | list | bool | None
json_str: TypeAlias = str

HTTPResponse = TypedDict(
    "HTTPResponse",
    {
        "isBase64Encoded": bool,
        "statusCode": HTTPStatus,
        "statusDescription": str,
        "headers": dict[str, str],
        "body": json_str,
    },
)


def build_response(
    http_status: HTTPStatus, body: dict[str, json_types]
) -> HTTPResponse:
    response: HTTPResponse = {
        "isBase64Encoded": False,
        "statusCode": http_status,
        "statusDescription": f"{http_status.value} {http_status.phrase}",
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }
    logger.info("Response: %s", response)
    return response


@runtime_checkable
class HTTPException(Protocol):
    http_status: HTTPStatus


class BadRequest(Exception):
    http_status = HTTPStatus.BAD_REQUEST


class Panic(Exception):
    http_status = HTTPStatus.INTERNAL_SERVER_ERROR


def format_validation_errors(e: ValidationError) -> dict[str, set]:
    logger.debug("formatting exception errors: %s", e.errors())
    reasons = defaultdict(set)

    for err in e.errors():
        field = ".".join(cast(tuple, err["loc"]))
        ctx = err.get("ctx", {})

        if err["type"] == "value_error.missing":
            reasons["possibly_missing"].add(field)
        elif err["type"] == "value_error.const":
            reasons["possibly_invalid"].add(
                (field, ctx.get("given"), ctx.get("permitted"))
            )
        else:
            reasons["other"].add((field, tuple(err.items())))
    return dict(reasons)


def exception_handler(ex: Exception, extra: dict[str, json_types] | None = None):
    logger.exception(ex, extra=extra)
    if isinstance(ex, HTTPException):
        return build_response(ex.http_status, {"exception": str(ex), "extra": extra})
    else:
        return build_response(
            HTTPStatus.INTERNAL_SERVER_ERROR, {"exception": str(ex), "extra": extra}
        )


class ConnectWithSecretModel(ConnectModel):
    secret: str | None

    @root_validator(pre=True)
    def populate_from_secret(cls, values):
        if "secret" in values:
            try:
                secret_value = parameters.get_secret(values["secret"], transform="json")
            except Exception as e:
                raise ValueError(
                    f"Failed to fetch or parse secret: {values['secret']} "
                    f"reason: {str(e)}"
                ) from e
            else:
                values.update(secret_value)
        return values


class LambdaRequest(Request):
    connection: ConnectWithSecretModel


class Envelope(BaseModel, extra=Extra.allow):
    body: Json[LambdaRequest]
    isBase64Encoded: bool

    @root_validator(pre=True)
    def prepare_data(cls, values):
        if values.get("isBase64Encoded"):
            encoded = values.get("body")
            logger.debug("Decoding base64 request body before parsing")
            payload = base64.b64decode(encoded).decode("utf-8")
            values["body"] = json.loads(json.dumps(payload))
        return values


def request_handler(event: LambdaRequest, context: LambdaContext) -> HTTPResponse:
    logger.debug("LambdaRequest: %s", repr(event))
    try:
        return build_response(HTTPStatus.OK, json.loads(DataPump.submit(event).json()))
    except Exception as e:
        return exception_handler(e)


@event_parser(model=Envelope)
def envelope_handler(event: Envelope, context: LambdaContext) -> HTTPResponse:
    return request_handler(parse_obj_as(LambdaRequest, event.body), context)


@logger.inject_lambda_context
def lambda_handler(event: dict, context: LambdaContext) -> HTTPResponse:
    """
    sample submit:
    event = {
        "connection": {
            "username": HR,
            "password": "HR",
            "hostname": "somehost@domain.com",
            "database": "ORCLPDB1",
            "secret": "Optional AWS SecretsManger secret name/arn with the above fields"
        },
        "request": "SUBMIT",
        "payload": {
            "operation": "EXPORT",
            "mode": "SCHEMA",
            "directives": [
                {"name": "PARALLEL", "value": 2},
                {"name": "COMPRESSION", "value": "ALL"},
                {"name": "INCLUDE_SCHEMA", "value": "HR"}
            ]
        }
    sample status:
    event = {
        "connection": {
            "username": HR,
            "password": "HR",
            "hostname": "somehost@domain.com",
            "database": "ORCLPDB1",
            "secret": "Optional AWS SecretsManger secret name/arn with the above fields"
        },
        "request": "STATUS",
        "payload": {
            "job_name": "EXP-HR-20230206222426382048",
            "job_owner": "HR",
        }
    }
    """
    logger.set_correlation_id(context.aws_request_id)

    envelope_validation_exc: ValidationError | None = None
    if ENVELOPE:
        # Extract the request from outer envelope supplied as an env arg. Valid args
        # could potentially be any one of:
        # https://awslabs.github.io/aws-lambda-powertools-python/2.9.1/utilities/parser/#built-in-models
        # Currently the expectation is that the outer envelope is a AlbModel or
        # APIGatewayProxyEventModel
        logger.debug("ENVELOPE=%s", ENVELOPE)
        expected_envelope = getattr(models, ENVELOPE)
        try:
            envelope_request = parse(event=event, model=expected_envelope)
            return envelope_handler(envelope_request, context)
        except ValidationError as e:
            # We might have been passed an un-enveloped request
            logger.info(
                f"Envelope validation failed for {ENVELOPE}! Attempting raw request "
                "validation..."
            )
            envelope_validation_exc = e

    try:
        return request_handler(parse_obj_as(LambdaRequest, event), context)
    except ValidationError as raw_validation_exc:
        ee = (
            format_validation_errors(envelope_validation_exc)
            if envelope_validation_exc
            else None
        )
        re = format_validation_errors(raw_validation_exc)
        exc = BadRequest(
            {"RawValidationException": re, "EnvelopeValidationException": ee}
        )
        return exception_handler(exc)
