import logging

import pytest

from oracledb_datapump.client import DataPump
from oracledb_datapump.database import ConnectDict
from oracledb_datapump.request import Request
from tests.constants import DUMPFILE_CACHE

logger = logging.getLogger(__name__)


def run_schema_import_wait(
    connect_params: ConnectDict,
    dumpfiles: list[str],
    from_schema: str,
    to_schema: str,
    from_tblspc: str,
    to_tblspc: str,
    parallel=2,
    no_user=False,
):
    logger.info(
        "TEST SCHEMA IMPORT (WAIT): %s %s %s %d %s",
        dumpfiles,
        f"{from_schema}:{to_schema}",
        f"{from_tblspc}:{to_tblspc}",
        parallel,
        no_user,
    )

    _dumpfiles = dumpfiles

    job_request = {
        "connection": connect_params,
        "request": "SUBMIT",
        "payload": {
            "operation": "IMPORT",
            "mode": "SCHEMA",
            "wait": True,
            "dumpfiles": _dumpfiles,
            "directives": [
                {"name": "PARALLEL", "value": parallel},
                {"name": "TABLE_EXISTS_ACTION", "value": "REPLACE"},
                {"name": "OID", "value": False},
                {"name": "REMAP_SCHEMA", "old_value": from_schema, "value": to_schema},
                {
                    "name": "REMAP_TABLESPACE",
                    "old_value": from_tblspc,
                    "value": to_tblspc,
                },
                {"name": "DELETE_FILES", "value": True},
            ],
        },
    }

    if no_user:
        job_request["payload"]["directives"].append(  # type: ignore
            {"name": "EXCLUDE_OBJECT_TYPE", "value": "USER"}
        )

    logger.debug("Request data is: %s", job_request)
    job_response = DataPump.submit(Request(**job_request))
    logger.info("job_response: %s", job_response)

    return job_response


def run_schema_import_nowait(
    connect_params: ConnectDict,
    dumpfiles: str,
    from_schema: str,
    to_schema: str,
    from_tblspc: str,
    to_tblspc: str,
    parallel=2,
    no_user=False,
):
    logger.info(
        "TEST SCHEMA IMPORT ASYNC: %s %s %s %d %s",
        dumpfiles,
        f"{from_schema}:{to_schema}",
        f"{from_tblspc}:{to_tblspc}",
        parallel,
        no_user,
    )

    _dumpfiles = dumpfiles

    job_request = {
        "connection": connect_params,
        "request": "SUBMIT",
        "payload": {
            "operation": "IMPORT",
            "mode": "SCHEMA",
            "wait": False,
            "dumpfiles": _dumpfiles,
            "directives": [
                {"name": "PARALLEL", "value": parallel},
                {"name": "TABLE_EXISTS_ACTION", "value": "REPLACE"},
                {"name": "OID", "value": False},
                {"name": "REMAP_SCHEMA", "old_value": from_schema, "value": to_schema},
                {
                    "name": "REMAP_TABLESPACE",
                    "old_value": from_tblspc,
                    "value": to_tblspc,
                },
            ],
        },
    }

    if no_user:
        job_request["payload"]["directives"].append(  # type: ignore
            {"name": "EXCLUDE_OBJECT_TYPE", "value": "USER"}
        )

    logger.debug("Request data is: %s", job_request)
    job_response = DataPump.submit(Request(**job_request))
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

    status_response = DataPump.submit(Request(**status_request))
    logger.info("status_response: %s", status_response)

    response = DataPump.poll_for_completion(
        connect_params, job_response.job_name, job_response.job_owner, rate=10
    )
    logger.info(response)

    return response


@pytest.mark.parametrize("parallel, wait, no_user", [(2, True, True)])
def test_schema_import(
    connect_params,
    pytestconfig,
    dumpfiles,
    from_schema,
    to_schema,
    from_tblspc,
    to_tblspc,
    parallel,
    wait,
    no_user,
):
    if wait:
        response = run_schema_import_wait(
            connect_params,
            dumpfiles,
            from_schema,
            to_schema,
            from_tblspc,
            to_tblspc,
            parallel,
            no_user,
        )
    else:
        response = run_schema_import_nowait(
            connect_params,
            dumpfiles,
            from_schema,
            to_schema,
            from_tblspc,
            to_tblspc,
            parallel,
            no_user,
        )

    pytestconfig.cache.set(DUMPFILE_CACHE, None)
    assert response.state == "COMPLETED"
