from typing import IO

import pydantic

from oracledb_datapump.base import ConnectDict
from oracledb_datapump.database import Connection, get_connection
from oracledb_datapump.files import DumpFile, OracleFile, SupportedOpenModes, ora_open
from oracledb_datapump.log import get_logger
from oracledb_datapump.request import (
    JsonStr,
    Request,
    RequestHandler,
    Response,
    dumpfile_info_builder,
)

logger = get_logger(__name__)


class DataPump:
    @classmethod
    def submit(cls, request: Request | JsonStr) -> Response:
        if isinstance(request, JsonStr):
            request = pydantic.parse_raw_as(Request, request)

        handler = RequestHandler()
        return handler.handle(request)

    @classmethod
    def open_file(
        cls,
        __file: str,
        /,
        mode: SupportedOpenModes,
        connection: str | ConnectDict | Connection,
        encoding: str | None = None,
    ) -> IO:
        connection = get_connection(connection)
        ora_file = OracleFile(
            file=__file,
            connection=connection,
        )

        return ora_open(ora_file, mode, encoding)

    @classmethod
    def get_dumpfile_info(
        cls, dumpfile: str, connection: str | ConnectDict | Connection
    ):
        connection = get_connection(connection)
        file = DumpFile(dumpfile, connection)
        info, file_type = file.get_info()
        return dumpfile_info_builder(info, file_type)  # type: ignore

    @classmethod
    def get_logfile(
        cls, logfile: str, connection: str | ConnectDict | Connection
    ) -> str:
        with cls.open_file(logfile, mode="r", connection=connection) as fh:
            return fh.read()

    @classmethod
    def poll_for_completion(
        cls,
        connection: ConnectDict,
        job_name: str,
        job_owner: str,
        rate: int = 30,
    ) -> Response:
        request = {
            "connection": connection,
            "request": "POLL",
            "payload": {"job_name": job_name, "job_owner": job_owner, "rate": rate},
        }
        return cls.submit(Request(**request))
