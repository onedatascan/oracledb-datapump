from enum import Enum, StrEnum
from typing import NewType, NotRequired, TypedDict


class Operation(StrEnum):
    IMPORT = "IMPORT"
    EXPORT = "EXPORT"
    SQL_FILE = "SQL_FILE"

    def abbrv(self):
        return self.value[:3]

    @classmethod
    def _missing_(cls, value: str):
        for member in cls:
            if member.value == value.upper():
                return member


class JobMode(StrEnum):
    FULL = "FULL"
    SCHEMA = "SCHEMA"
    TABLE = "TABLE"
    TABLESPACE = "TABLESPACE"
    TRANSPORTABLE = "TRANSPORTABLE"

    @classmethod
    def _missing_(cls, value: str):
        for member in cls:
            if member.value == value.upper():
                return member


class Stage(Enum):
    PRE = 1
    DATAPUMP = 2
    POST = 3


DataPumpHandle = NewType("DataPumpHandle", int)


class JobMetaData(TypedDict):
    job_name: str
    job_owner: str
    job_date: str
    executed_by: str
    operation: str
    mode: str
    schemas: list[str]
    schema_versions: dict[str, str]
    tablespaces: list[str]
    dumpfiles: list[str]
    directory: str
    directives: list[str]


class ConnectDict(TypedDict):
    username: str
    password: str
    hostname: str
    database: str
    port: NotRequired[int]
