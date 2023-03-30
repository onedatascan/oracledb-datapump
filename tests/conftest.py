import logging
import sys
from pathlib import Path
from typing import cast

import pytest
from dotenv import dotenv_values

from tests.constants import DUMPFILE_CACHE

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).parent.parent
sys.path.append(str(REPO_ROOT))

# Expects a '.env' file at cwd containing environ vars like:
#     export DB_HOST='localhost'
#     export DATABASE='ORCLPDB1'
#     export ADMIN_USER='SYSTEM'
#     export ORACLE_PWD='manager'
#     export PARALLEL=2
#     export SCHEMA1='TEST1'
#     export SCHEMA2='TEST2'
#     export SCHEMA1_TBLSPC='TEST_DATA1'
#     export SCHEMA2_TBLSPC='TEST_DATA2'


def mock_env_config() -> dict[str, str | int]:
    config_map = dotenv_values(verbose=True)
    assert config_map["DB_HOST"]
    assert config_map["DATABASE"]
    assert config_map["ADMIN_USER"]
    assert config_map["ORACLE_PWD"]
    assert config_map["PARALLEL"]
    assert config_map["SCHEMA1"]
    assert config_map["SCHEMA2"]
    assert config_map["SCHEMA1_TBLSPC"]
    assert config_map["SCHEMA2_TBLSPC"]
    return cast(dict[str, str | int], config_map)


@pytest.fixture(scope="session", autouse=True)
def env_config():
    return mock_env_config()


@pytest.fixture
def from_schema(request, env_config):
    marker = request.node.get_closest_marker("from_schema")
    if not marker:
        return env_config["SCHEMA1"]
    else:
        return marker.args[0]


@pytest.fixture
def to_schema(request, env_config):
    marker = request.node.get_closest_marker("to_schema")
    if not marker:
        return env_config["SCHEMA2"]
    else:
        return marker.args[0]


@pytest.fixture
def from_tblspc(request, env_config):
    marker = request.node.get_closest_marker("from_tblspc")
    if not marker:
        return env_config["SCHEMA1_TBLSPC"]
    else:
        return marker.args[0]


@pytest.fixture
def to_tblspc(request, env_config):
    marker = request.node.get_closest_marker("to_tblspc")
    if not marker:
        return env_config["SCHEMA2_TBLSPC"]
    else:
        return marker.args[0]


@pytest.fixture
def dumpfiles(request):
    val = request.config.cache.get(DUMPFILE_CACHE, None)
    if val is None:
        raise RuntimeError("Test dumpfile cache not populated. Run import test first.")
    return val


@pytest.fixture(scope="session", autouse=True)
def db_ctx(env_config):
    from oracledb_datapump.database import ConnectDict, get_connection

    return get_connection(
        ConnectDict(
            user=str(env_config["ADMIN_USER"]),
            password=str(env_config["ORACLE_PWD"]),
            host=str(env_config["DB_HOST"]),
            database=str(env_config["DATABASE"]),
        )
    )


@pytest.fixture(scope="session", autouse=True)
def connect_params(env_config):
    from oracledb_datapump.database import ConnectDict

    return ConnectDict(
        user=str(env_config["ADMIN_USER"]),
        password=str(env_config["ORACLE_PWD"]),
        host=str(env_config["DB_HOST"]),
        database=str(env_config["DATABASE"]),
    )


@pytest.fixture
def make_import_request(env_config):

    requests = []

    def _make_import_request(
        dumpfiles: list[str] | None = None,
        filters: list[dict] | None = None,
        options: list[dict] | None = None,
        parameters: list[dict] | None = None,
        remaps: list[dict] | None = None,
        transforms: list[dict] | None = None,
    ):
        request = {
            "connection": {
                "user": env_config["ADMIN_USER"],
                "password": env_config["ORACLE_PWD"],
                "host": env_config["DB_HOST"],
                "database": env_config["DATABASE"],
            },
            "request": "SUBMIT",
            "payload": {
                "operation": "import",
                "mode": "schema",
                "dumpfiles": dumpfiles,
                "directives": [filters, options, parameters, remaps, transforms],
            },
        }
        requests.append(request)
        return request

    yield _make_import_request
