from typing import Final, Literal, Type, TypeAlias

DEFAULT_DP_DIR: Final[str] = "DATA_PUMP_DIR"
DATE_STR_FMT: Final[str] = "%Y%m%d%H%M%S%f"
ISO_TIMESTAMP_MASK: Final[str] = 'YYYY-MM-DD"T"HH24:MI:SS.FF'
DMP_CREATE_DATE_STR_FMT: Final[str] = "%c"
SUB_SEQ_VAR: Final[str] = "%U"
FLYWAY_SCHEMA_HISTORY_ENV: Final[str] = "FLYWAY_SCHEMA_HISTORY"
EMPTY_BYTE: Final[bytes] = b""
EMPTY_STR: Final[str] = ""
LOB_FETCH_SIZE_MULT: Final[int] = 8
RAW_MAX_BYTES: Final[int] = 32767
ARG_DELIMITER: Final[str] = ":"
SERVICE_NAME: Final[str] = "oracledb-datapump"
STATUS_TIMEOUT: Final[int] = 120
DEFAULT_SQLNET_PORT: Final[int] = 1521
NAME_DELIM: Final[str] = "-"

IOMode = Literal["r", "w"]
ContentType: TypeAlias = Type[str] | Type[bytes]
