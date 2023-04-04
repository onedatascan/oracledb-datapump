from __future__ import annotations

import abc
import functools
import operator
from datetime import datetime
from enum import IntFlag, StrEnum
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    Iterator,
    Optional,
    Self,
    TypeVar,
)

from oracledb_datapump import constants, sql
from oracledb_datapump.base import JobMetaData, JobMode, Stage
from oracledb_datapump.context import JobContext, OpenContext
from oracledb_datapump.database import Schema, to_db_timezone
from oracledb_datapump.exceptions import InvalidObjectType
from oracledb_datapump.files import OracleFile
from oracledb_datapump.log import get_logger
from oracledb_datapump.util import parse_dt

logger = get_logger(__name__)


class DirectiveBase:
    stage: ClassVar[Stage]
    name: str | None
    value: datetime | bool | int | str
    registry: dict[str, type[Self]] = {}

    def __init_subclass__(cls, name: str | None) -> None:
        cls.name = name
        if name is not None:
            DirectiveBase.registry[name] = cls
        return super().__init_subclass__()

    @abc.abstractmethod
    def apply(self, ctx: JobContext) -> None:
        ...

    @property
    def metadata(self) -> dict:
        return {"directives": [repr(self)]}


class DirectiveEnums(type):
    """
    Metaclass that sets annotated class variables as
    instances of the enclosing class using the variable
    name as the constructor arg.
    """

    def __init__(cls, clsname, bases, clsdict, **kwargs):
        annotations = clsdict.get("__annotations__", {})
        for attr, _type in annotations.items():
            if _type == "ClassVar[Self]":
                setattr(cls, attr, cls(attr))


class _DataPumpExprFilter(StrEnum):
    NAME_EXPR = "NAME_EXPR"
    SCHEMA_EXPR = "SCHEMA_EXPR"
    TABLESPACE_EXPR = "TABLESPACE_EXPR"


class _DataPumpListFilter(StrEnum):
    NAME_LIST = "NAME_LIST"
    SCHEMA_LIST = "SCHEMA_LIST"
    TABLESPACE_LIST = "TABLESPACE_LIST"


class _DataPumpExprFilterPath(StrEnum):
    INCLUDE_PATH_EXPR = "INCLUDE_PATH_EXPR"
    EXCLUDE_PATH_EXPR = "EXCLUDE_PATH_EXPR"


class _DataPumpListPath(StrEnum):
    INCLUDE_PATH_LIST = "INCLUDE_PATH_LIST"
    EXCLUDE_PATH_LIST = "EXCLUDE_PATH_LIST"


class Filter(DirectiveBase, name=None):
    stage = Stage.DATAPUMP
    name: str
    kind: StrEnum

    @abc.abstractmethod
    def apply(self, ctx: OpenContext):
        ...


class ExpressionFilter(Filter, name=None):
    kind: _DataPumpExprFilter | _DataPumpExprFilterPath
    value: str
    object_path: str | None

    def __init__(self, value: str, object_path: str | None = None):
        self.value = value.upper()
        self.object_path = object_path.upper() if object_path else None

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            + f"{self.kind}, {self.value}, {self.object_path})"
        )

    @property
    @abc.abstractmethod
    def expression(self) -> str:
        ...

    @abc.abstractmethod
    def validate(self, ctx: OpenContext) -> None:
        ...

    def apply(self, ctx: OpenContext) -> None:
        self.validate(ctx)

        with ctx.connection.cursor() as cursor:
            cursor.callproc(
                name="DBMS_DATAPUMP.METADATA_FILTER",
                keyword_parameters={
                    "handle": int(ctx.job_handle),
                    "name": str(self.kind),
                    "value": self.expression,
                    "object_path": self.object_path,
                },
            )


class ExcludeObjectType(ExpressionFilter, name="EXCLUDE_OBJECT_TYPE"):
    kind = _DataPumpExprFilterPath.EXCLUDE_PATH_EXPR

    @property
    def expression(self) -> str:
        return "IN (" + f"'{self.value}'" + ")"

    def validate(self, ctx: OpenContext) -> None:
        validate_types(ctx=ctx, object_type=self.value)


class IncludeSchema(ExpressionFilter, name="INCLUDE_SCHEMA"):
    kind = _DataPumpExprFilter.SCHEMA_EXPR
    value: Schema
    object_path = None

    def __init__(self, value: Schema | str):
        if isinstance(value, Schema):
            self.value = value
        else:
            self.value = Schema(value)

    @property
    def expression(self) -> str:
        return f"IN ('{self.value.name.upper()}')"

    def validate(self, ctx: OpenContext) -> None:
        pass


class IncludeTable(ExpressionFilter, name="INCLUDE_TABLE"):
    kind = _DataPumpExprFilter.NAME_EXPR
    value: str
    object_path = "TABLE"

    def __init__(self, value: str):
        self.value = value

    @property
    def expression(self) -> str:
        return f"IN ('{self.value.upper()}')"

    def validate(self, ctx: OpenContext) -> None:
        pass


T = TypeVar("T", str, int, bool, datetime, list["DataOptionsParams"])


class Parameter(DirectiveBase, Generic[T], name=None):
    stage: ClassVar[Stage] = Stage.DATAPUMP
    value: T
    name: str

    def __init__(self, value: T):
        if isinstance(value, str):
            self.value = value.upper()
        elif isinstance(value, bool):
            self.value = int(value)  # type: ignore
        else:
            self.value = value

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name}, {self.value})"

    def apply(self, ctx: OpenContext) -> None:
        if isinstance(self.value, datetime):
            dt = to_db_timezone(self.value, ctx.connection).replace(tzinfo=None)
            self.value = (
                f"to_timestamp('{dt.isoformat()}', '{constants.ISO_TIMESTAMP_MASK}')"
            )  # type: ignore
        with ctx.connection.cursor() as cursor:
            cursor.callproc(
                name="DBMS_DATAPUMP.SET_PARAMETER",
                keyword_parameters={
                    "handle": int(ctx.job_handle),
                    "name": str(self.name),
                    "value": self.value,
                },
            )


class ParameterTypeWithUserArgs(Parameter[T], name=None):
    ...


class ParameterTypeWithEnumArgs(Parameter[str], metaclass=DirectiveEnums, name=None):
    ...


class DataOptionsParams(IntFlag):
    SKIP_CONST_ERR = 1
    XMLTYPE_CLOB = 2
    NO_TYPE_EVOL = 4
    DISABL_APPEND_HINT = 8
    REJECT_ROWS_REPCHR = 16
    ENABLE_NET_COMP = 32
    GRP_PART_TAB = 64
    TRUST_EXIST_TB_PAR = 128
    VALIDATE_TBL_DATA = 256
    VERIFY_STREAM_FORM = 512
    CONT_LD_ON_FMT_ERR = 1024


class ClientCommand(ParameterTypeWithUserArgs[str], name="CLIENT_COMMAND"):
    ...


class Compression(ParameterTypeWithEnumArgs, name="COMPRESSION"):
    DATA_ONLY: ClassVar[Self]
    METADATA_ONLY: ClassVar[Self]  # default
    ALL: ClassVar[Self]
    NONE: ClassVar[Self]


class CompressionAlgorithm(ParameterTypeWithEnumArgs, name="COMPRESSION_ALGORITHM"):
    """Requires Oracle Advanced Compression option"""

    BASIC: ClassVar[Self]
    LOW: ClassVar[Self]
    MEDIUM: ClassVar[Self]
    HIGH: ClassVar[Self]


class DataAcessMethod(ParameterTypeWithEnumArgs, name="DATA_ACCESS_METHOD"):
    AUTOMATIC: ClassVar[Self]
    DIRECT_PATH: ClassVar[Self]
    EXTERNAL_TABLE: ClassVar[Self]


class DataOptions(
    ParameterTypeWithUserArgs[list[DataOptionsParams]], name="DATA_OPTIONS"
):
    def __init__(self, value: list[DataOptionsParams]):
        self.value: int = functools.reduce(operator.or_, value, 0)


class Encryption(ParameterTypeWithEnumArgs, name="ENCRYPTION"):
    ALL: ClassVar[Self]
    DATA_ONLY: ClassVar[Self]
    ENCRYPTED_COLUMNS_ONLY: ClassVar[Self]
    METADATA_ONLY: ClassVar[Self]
    NONE: ClassVar[Self]


class EncryptionAlgorithm(ParameterTypeWithEnumArgs, name="ENCRYPTION_ALGORITHM"):
    AES128: ClassVar[Self]
    AES192: ClassVar[Self]
    AES256: ClassVar[Self]


class EncryptionMode(ParameterTypeWithEnumArgs, name="ENCRYPTION_MODE"):
    PASSWORD: ClassVar[Self]
    TRANSPARENT: ClassVar[Self]
    DUAL: ClassVar[Self]


class EncryptionPassword(ParameterTypeWithEnumArgs, name="ENCRYPTION_PASSWORD"):
    PASSWORD: ClassVar[Self]
    DUAL: ClassVar[Self]


class Estimate(ParameterTypeWithEnumArgs, name="ESTIMATE"):
    BLOCKS: ClassVar[Self]
    STATISTICS: ClassVar[Self]


class EstimateOnly(ParameterTypeWithUserArgs[int], name="ESTIMATE_ONLY"):
    ...


class FlashbackScn(ParameterTypeWithUserArgs[int], name="FLASHBACK_SCN"):
    @property
    def metadata(self) -> dict:
        return {"as_of": f"SCN({self.value})"}


class FlashbackTime(ParameterTypeWithUserArgs[datetime], name="FLASHBACK_TIME"):
    def __init__(self, value: str | datetime):
        self.value: datetime = parse_dt(value)

    @property
    def metadata(self) -> dict:
        return {"as_of": self.value.isoformat()}


class IncludeMetadata(ParameterTypeWithUserArgs[bool], name="INCLUDE_METADATA"):
    ...


class KeepMaster(ParameterTypeWithUserArgs[bool], name="KEEP_MASTER"):
    ...


class Logtime(ParameterTypeWithEnumArgs, name="LOGTIME"):
    NONE: ClassVar[Self]  # default
    STATUS: ClassVar[Self]
    LOGFILE: ClassVar[Self]
    ALL: ClassVar[Self]


class MasterOnly(ParameterTypeWithUserArgs[bool], name="MASTER_ONLY"):
    ...


class Metrics(ParameterTypeWithUserArgs[bool], name="METRICS"):
    ...


class PartitionOptions(ParameterTypeWithEnumArgs, name="PARTITION_OPTIONS"):
    NONE: ClassVar[Self]  # default
    DEPARTITION: ClassVar[Self]
    MERGE: ClassVar[Self]


class ReuseDatafiles(ParameterTypeWithUserArgs[bool], name="REUSE_DATAFILES"):
    ...


class SkipUnusableIndexes(
    ParameterTypeWithUserArgs[bool], name="SKIP_UNUSABLE_INDEXES"
):
    ...


class SourceEdition(ParameterTypeWithUserArgs[bool], name="SOURCE_EDITION"):
    ...


class StreamsConfiguraion(
    ParameterTypeWithUserArgs[bool], name="STREAMS_CONFIGURATION"
):
    ...


class TableExistsAction(ParameterTypeWithEnumArgs, name="TABLE_EXISTS_ACTION"):
    TRUNCATE: ClassVar[Self]
    REPLACE: ClassVar[Self]
    APPEND: ClassVar[Self]
    SKIP: ClassVar[Self]


class TablespaceDatafile(ParameterTypeWithEnumArgs, name="TABLESPACE_DATAFILE"):
    TABLESPACE_DATAFILE: ClassVar[Self]


class TargetEdition(ParameterTypeWithUserArgs[str], name="TARGET_EDITION"):
    ...


class Transportable(ParameterTypeWithEnumArgs, name="TRANSPORTABLE"):
    ALWAYS: ClassVar[Self]
    NEVER: ClassVar[Self]


class TtsFullCheck(ParameterTypeWithUserArgs[bool], name="TTS_FULL_CHECK"):
    ...


class UserMetadata(ParameterTypeWithUserArgs[bool], name="USER_METADATA"):
    ...


class Parallel(ParameterTypeWithUserArgs[int], name="PARALLEL"):
    def apply(self, ctx: OpenContext) -> None:
        with ctx.connection.cursor() as cursor:
            cursor.callproc(
                name="DBMS_DATAPUMP.SET_PARALLEL",
                keyword_parameters={
                    "handle": int(ctx.job_handle),
                    "degree": self.value,
                },
            )


class Remap(DirectiveBase, name=None):
    stage: ClassVar[Stage] = Stage.DATAPUMP
    name: str
    old_value: str
    value: str
    object_path: str | None

    def __init__(self, old_value: str, value: str, object_path: str | None = None):
        self.old_value = old_value.upper()
        self.value = value.upper()
        self.object_path = object_path.upper() if object_path else None

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            + f"{self.name}, {self.old_value}, {self.value}, {self.object_path})"
        )

    def validate(self, ctx: OpenContext) -> None:
        if self.object_path:
            validate_named_type(ctx, self.object_path)

    def apply(self, ctx: OpenContext) -> None:
        self.validate(ctx)
        with ctx.connection.cursor() as cursor:
            cursor.callproc(
                name="DBMS_DATAPUMP.METADATA_REMAP",
                keyword_parameters={
                    "handle": int(ctx.job_handle),
                    "name": str(self.name),
                    "old_value": self.old_value,
                    "value": self.value,
                    "object_type": self.object_path,
                },
            )


class RemapSchema(Remap, name="REMAP_SCHEMA"):
    ...


class RemapTablespace(Remap, name="REMAP_TABLESPACE"):
    ...


class RemapDatafile(Remap, name="REMAP_DATAFILE"):
    ...


class Transform(DirectiveBase, name=None):
    stage: ClassVar[Stage] = Stage.DATAPUMP
    name: str
    value: str | int | bool
    object_type: Optional[str]

    def __init__(self, value: str | int | bool, object_type: str | None = None):
        if isinstance(value, str):
            self.value = value.upper()
        if isinstance(value, bool):
            self.value = int(value)
        else:
            self.value = value
        self.object_type = object_type.upper() if object_type else None

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({self.name}, {self.value}, {self.object_type})"
        )

    def validate(self, ctx: OpenContext) -> None:
        if self.object_type:
            validate_named_type(ctx, self.object_type)

    def apply(self, ctx: OpenContext) -> None:
        self.validate(ctx)
        with ctx.connection.cursor() as cursor:
            cursor.callproc(
                name="DBMS_DATAPUMP.METADATA_TRANSFORM",
                keyword_parameters={
                    "handle": int(ctx.job_handle),
                    "name": str(self.name),
                    "value": self.value,
                    "object_type": self.object_type,
                },
            )


class LobStorageType(StrEnum):
    SECUREFILE = "SECUREFILE"
    BASICFILE = "BASICFILE"
    DEFAULT = "DEFAULT"
    NO_CHANGE = "NO_CHANGE"


class DisableArchiveLogging(Transform, name="DISABLE_ARCHIVE_LOGGING"):
    def __init__(self, value: bool, object_path: str | None = None):
        super().__init__(int(value), object_path)


class Inmemory(Transform, name="INMEMORY"):
    def __init__(self, value: bool, object_path: str | None = None):
        super().__init__(int(value), object_path)


class InmemoryClause(Transform, name="INMEMORY_CLAUSE"):
    def __init__(self, value: str, object_path: str | None = None):
        super().__init__(value, object_path)


class LobStore(Transform, name="LOB_STORAGE"):
    def __init__(self, value: LobStorageType, object_path: str | None = None):
        super().__init__(str(value), object_path)


class Oid(Transform, name="OID"):
    def __init__(self, value: bool, object_path: str | None = None):
        super().__init__(int(value), object_path)


class Pctspace(Transform, name="PCTSPACE"):
    def __init__(self, value: int, object_path: str | None = None):
        super().__init__(value, object_path)


class SegmentAttributes(Transform, name="SEGMENT_ATTRIBUTES"):
    def __init__(self, value: bool, object_path: str | None = None):
        super().__init__(int(value), object_path)


class SegmentCreation(Transform, name="SEGMENT_CREATION"):
    def __init__(self, value: bool, object_path: str | None = None):
        super().__init__(int(value), object_path)


class Storage(Transform, name="STORAGE"):
    def __init__(self, value: bool, object_path: str | None = None):
        super().__init__(int(value), object_path)


class TableCompressionClause(Transform, name="TABLE_COMPRESSION_CLAUSE"):
    def __init__(self, value: str, object_path: str | None = None):
        super().__init__(value, object_path)


class Extra(DirectiveBase, name=None):
    stage: ClassVar[Stage]
    name: str

    def __init__(self, value, **kwargs):
        self.value = value
        self.kwargs = kwargs
        self._job_metadata = None

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name}, {self.value}, {self.kwargs})"

    @property
    def job_metadata(self) -> JobMetaData | None:
        return self._job_metadata

    @job_metadata.setter
    def job_metadata(self, job_metadata: JobMetaData) -> None:
        self._job_metadata = job_metadata


class DeleteFiles(Extra, name="DELETE_FILES"):
    stage: ClassVar[Stage] = Stage.POST
    name: str

    def apply(self, ctx: OpenContext) -> None:
        logger.debug("Deleting files from job: %s", self.job_metadata)
        assert self.job_metadata
        for file in self.job_metadata["dumpfiles"]:
            orafile = OracleFile(file, connection=ctx.connection)
            orafile.handler.delete()


@functools.lru_cache(maxsize=32)
def get_object_type_lookup_table(mode: JobMode) -> str:
    match mode:
        case JobMode.FULL:
            lookup_table = "DATABASE_EXPORT_OBJECTS"
        case JobMode.SCHEMA:
            lookup_table = "SCHEMA_EXPORT_OBJECTS"
        case JobMode.TABLE:
            lookup_table = "TABLE_EXPORT_OBJECTS"
        case JobMode.TABLESPACE:
            lookup_table = "TABLESPACE_EXPORT_OBJECTS"
        case _:
            raise ValueError("Invalid Mode %s", mode)
    return lookup_table


def get_dp_object_types(ctx: OpenContext) -> Iterator[tuple[Any, ...]]:
    lookup_table = get_object_type_lookup_table(ctx.mode)
    with ctx.connection.cursor() as cursor:
        cursor.execute(sql.SQL_GET_EXPORT_OBJS, [lookup_table])
        for row in cursor:
            yield row


def validate_named_type(ctx: OpenContext, object_type: str) -> None:
    expr_obj_predicates = filter(lambda i: i[2] == "Y", get_dp_object_types(ctx))
    valid_object_types = [str(r[0]) for r in expr_obj_predicates]

    if object_type not in valid_object_types:
        raise InvalidObjectType(object_type, valid_object_types)


def validate_types(ctx: OpenContext, object_type: str) -> None:
    valid_object_types = [str(r[0]) for r in get_dp_object_types(ctx)]

    if object_type not in valid_object_types:
        raise InvalidObjectType(object_type, valid_object_types)


_T = TypeVar("_T")


class ClassGetAttr(Generic[_T], type):
    _registry: dict[str, type[_T]]

    def __getattr__(cls, clsattr: str) -> type[_T]:
        try:
            return cls._registry[clsattr]
        except KeyError:
            raise AttributeError(clsattr)


class Directive(metaclass=ClassGetAttr[DirectiveBase]):
    _registry = DirectiveBase.registry

    if TYPE_CHECKING:
        EXCLUDE_OBJECT_TYPE: type[ExcludeObjectType]
        INCLUDE_SCHEMA: type[IncludeSchema]
        INCLUDE_TABLE: type[IncludeTable]
        CLIENT_COMMAND: type[ClientCommand]
        COMPRESSION: type[Compression]
        COMPRESSION_ALGORITHM: type[CompressionAlgorithm]
        DATA_ACCESS_METHOD: type[DataAcessMethod]
        DATA_OPTIONS: type[DataOptions]
        ENCRYPTION: type[Encryption]
        ENCRYPTION_ALGORITHM: type[EncryptionAlgorithm]
        ENCRYPTION_MODE: type[EncryptionMode]
        ENCRYPTION_PASSWORD: type[EncryptionPassword]
        ESTIMATE: type[Estimate]
        ESTIMATE_ONLY: type[EstimateOnly]
        FLASHBACK_SCN: type[FlashbackScn]
        FLASHBACK_TIME: type[FlashbackTime]
        INCLUDE_METADATA: type[IncludeMetadata]
        KEEP_MASTER: type[KeepMaster]
        LOGTIME: type[Logtime]
        MASTER_ONLY: type[MasterOnly]
        METRICS: type[Metrics]
        PARTITION_OPTIONS: type[PartitionOptions]
        REUSE_DATAFILES: type[ReuseDatafiles]
        SKIP_UNUSABLE_INDEXES: type[SkipUnusableIndexes]
        SOURCE_EDITION: type[SourceEdition]
        STREAMS_CONFIGURATION: type[StreamsConfiguraion]
        TABLE_EXISTS_ACTION: type[TableExistsAction]
        TABLESPACE_DATAFILE: type[TablespaceDatafile]
        TARGET_EDITION: type[TargetEdition]
        TRANSPORTABLE: type[Transportable]
        TTS_FULL_CHECK: type[TtsFullCheck]
        USER_METADATA: type[UserMetadata]
        PARALLEL: type[Parallel]
        REMAP_SCHEMA: type[RemapSchema]
        REMAP_TABLESPACE: type[RemapTablespace]
        REMAP_DATAFILE: type[RemapDatafile]
        DISABLE_ARCHIVE_LOGGING: type[DisableArchiveLogging]
        INMEMORY: type[Inmemory]
        INMEMORY_CLAUSE: type[InmemoryClause]
        LOB_STORAGE: type[LobStore]
        OID: type[Oid]
        PCTSPACE: type[Pctspace]
        SEGMENT_ATTRIBUTES: type[SegmentAttributes]
        SEGMENT_CREATION: type[SegmentCreation]
        STORAGE: type[Storage]
        TABLE_COMPRESSION_CLAUSE: type[TableCompressionClause]
        DELETE_FILES: type[DeleteFiles]


DT = TypeVar("DT", Filter, Parameter, Remap, Transform, Extra)
