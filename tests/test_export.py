import logging

import pytest

from oracledb_datapump.client import DataPump
from oracledb_datapump.database import ConnectDict
from oracledb_datapump.request import StatusRequest, SubmitRequest
from tests.constants import DUMPFILE_CACHE

logger = logging.getLogger(__name__)


def run_schema_export_sync(
    connect_params: ConnectDict,
    schema: str,
    parallel=2,
    no_data=False,
):
    logger.info(
        "TEST SCHEMA EXPORT (WAIT): %s %d %s",
        schema,
        parallel,
        no_data,
    )

    job_request = {
        "connection": connect_params,
        "request": "SUBMIT",
        "payload": {
            "operation": "EXPORT",
            "mode": "SCHEMA",
            "wait": True,
            "tag": "TEST",
            "directives": [
                {"name": "PARALLEL", "value": parallel},
                {"name": "COMPRESSION", "value": "ALL"},
                {"name": "INCLUDE_SCHEMA", "value": schema},
            ],
        },
    }

    if no_data:
        job_request["payload"]["directives"]["filters"] = {  # type: ignore
            "name": "EXCLUDE_OBJECT_TYPE",
            "value": "TABLE",
        }

    logger.debug("Request data is: %s", job_request)
    response = DataPump.submit(SubmitRequest(**job_request))
    logger.info(response)

    return response


def run_schema_export_async(
    connect_params: ConnectDict,
    schema: str,
    parallel=2,
    no_data=False,
):
    logger.info(
        "TEST SCHEMA EXPORT ASYNC: %s %d %s",
        schema,
        parallel,
        no_data,
    )

    job_request = {
        "connection": connect_params,
        "request": "SUBMIT",
        "payload": {
            "operation": "EXPORT",
            "mode": "SCHEMA",
            "wait": False,
            "directives": [
                {"name": "PARALLEL", "value": parallel},
                {"name": "COMPRESSION", "value": "ALL"},
                {"name": "INCLUDE_SCHEMA", "value": schema},
            ],
        },
    }

    if no_data:
        job_request["payload"]["directives"]["filters"] = {  # type: ignore
            "name": "EXCLUDE_OBJECT_TYPE",
            "value": "TABLE",
        }

    logger.debug("Request data is: %s", job_request)
    job_response = DataPump.submit(SubmitRequest(**job_request))
    logger.info("job_response: %s", job_response)

    status_request = {
        "connection": connect_params,
        "request": "STATUS",
        "payload": {
            "job_name": str(job_response.job_name),
            "job_owner": str(job_response.job_owner),
            "type": "ALL",
        },
    }

    status_response = DataPump.submit(StatusRequest(**status_request))
    logger.info("status_response: %s", status_response)

    response = DataPump.poll_for_completion(
        connect_params, job_response.job_name, job_response.job_owner, rate=10
    )
    response.dumpfiles = status_response.dumpfiles
    logger.info(response)

    return response


@pytest.mark.parametrize("parallel, wait, no_data", [(2, True, False)])
def test_schema_export(
    connect_params, pytestconfig, from_schema, parallel, wait, no_data
):
    if wait:
        response = run_schema_export_sync(
            connect_params, from_schema, parallel, no_data
        )
    else:
        response = run_schema_export_async(
            connect_params, from_schema, parallel, no_data
        )
    pytestconfig.cache.set(DUMPFILE_CACHE, response.dumpfiles)
    assert response.state == "COMPLETED"


@pytest.mark.parametrize("parallel, wait, no_data", [(2, False, False)])
def test_schema_export_nowait(
    connect_params, pytestconfig, from_schema, parallel, wait, no_data
):
    if wait:
        response = run_schema_export_sync(
            connect_params, from_schema, parallel, no_data
        )
    else:
        response = run_schema_export_async(
            connect_params, from_schema, parallel, no_data
        )
    pytestconfig.cache.set(DUMPFILE_CACHE, response.dumpfiles)
    assert response.state == "COMPLETED"
