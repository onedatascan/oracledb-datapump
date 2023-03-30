from __future__ import annotations

import builtins
import collections
import io
import os
import re
from codecs import iterdecode
from enum import IntEnum
from functools import cached_property
from os import PathLike
from pathlib import PurePath
from typing import (
    IO,
    Any,
    Callable,
    ClassVar,
    Generic,
    Iterable,
    Iterator,
    Literal,
    Mapping,
    Self,
    Sequence,
    TypeAlias,
    TypeVar,
    assert_never,
    cast,
    overload,
)

from oracledb_datapump import constants, sql
from oracledb_datapump.base import Operation
from oracledb_datapump.database import (
    DB_CLOB,
    DB_LOB,
    DB_OBJECT,
    DB_OBJECT_TYPE,
    Connection,
)
from oracledb_datapump.exceptions import DatabaseError, UsageError
from oracledb_datapump.log import get_logger
from oracledb_datapump.util import object_loads

logger = get_logger(__name__)

SupportedOpenModes = Literal["r", "w", "rt", "wt", "rb", "wb"]


def resolve_connection(
    connection: Connection | Callable[..., Connection]
) -> Connection:
    if callable(connection):
        return connection()
    else:
        return connection


class FileType(IntEnum):
    UNKNOWN = 0
    DUMP_FILE = 1
    BAD_FILE = 2
    LOG_FILE = 3
    SQL_FILE = 4
    URI_DUMP_FILE = 5
    META_FILE = 6
    DUMP_FILE_SET = 7

    @property
    def suffix(self) -> str:
        if self is FileType.DUMP_FILE:
            return ".dmp"
        if self is FileType.BAD_FILE:
            return ""
        if self is FileType.LOG_FILE:
            return ".log"
        if self is FileType.SQL_FILE:
            return ".sql"
        if self is FileType.URI_DUMP_FILE:
            return ""
        if self is FileType.META_FILE:
            return ".json"
        if self is FileType.UNKNOWN:
            return ""
        if self is FileType.DUMP_FILE_SET:
            raise NotImplementedError("Suffix is not applicable to dumpfile_set")
        else:
            assert_never(self)

    @property
    def content_type(self) -> constants.ContentType:
        if self is FileType.DUMP_FILE:
            return bytes
        else:
            return str


class OracleDirectory:
    """Oracle Database Directory"""

    def __init__(self, name: str, connection: Connection | None = None):
        self.name = name.upper()
        self.connection = connection

        if self.connection is not None:
            self.handler = OracleDirectoryHandler(name, self.connection)
        else:
            self.handler = None

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"

    @classmethod
    def default(cls, connection: Connection | None = None) -> Self:
        return cls(constants.DEFAULT_DP_DIR, connection)

    @classmethod
    def from_path(cls, path: PurePath | str, connection: Connection) -> Self:
        dir_name = OracleDirectoryHandler.get_dir_from_path(path, connection)
        return cls(dir_name, connection)

    @property
    def path(self) -> PurePath:
        if not self.handler:
            raise UsageError("Cannot lookup path without a database context!")
        return self.handler.path

    def has_handler(self) -> bool:
        return self.handler is not None

    def get_handler(self, connection: Connection):
        if not self.handler:
            self.connection = connection
            self.handler = OracleDirectoryHandler(self.name, self.connection)


class OracleDirectoryHandler:
    def __init__(
        self,
        ora_directory: str,
        connection: Connection | Callable[..., Connection],
    ):
        self.connection = resolve_connection(connection)
        self.ora_directory = ora_directory.upper()

    @classmethod
    def get_dir_from_path(
        cls,
        path: str | PurePath,
        connection: Connection | Callable[..., Connection],
    ) -> str:
        connection = resolve_connection(connection)

        logger.debug("Resolving directory from path: %s", str(path))
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL_GET_DIRECTORY_FROM_PATH, parameters={"dir_path": str(path)}
            )
            result = iter(cursor.fetchall())

            try:
                dir_rec = next(result)
                dir_name = dir_rec[0]
                logger.debug("Found directory %s", dir_name)
            except (StopIteration, IndexError):
                raise ValueError(f"Oracle directory not found! {str(path)}")

            return dir_name

    @cached_property
    def path(self) -> PurePath:
        with self.connection.cursor() as cursor:
            cursor.execute(
                sql.SQL_GET_DIRECTORY_PATH,
                parameters={"dir_name": str(self.ora_directory)},
            )
            result = iter(cursor.fetchall())

            try:
                dir_rec = next(result)
                dir_path = PurePath(dir_rec[0])
            except (StopIteration, IndexError):
                raise ValueError(
                    "Oracle directory not found! %s",
                    self.ora_directory,
                )
            return dir_path


class OracleFile:
    """Files in or to be sent to an Oracle Directory."""

    file_type = FileType.UNKNOWN

    def __init__(
        self,
        file: str | PurePath,
        connection: Connection,
        directory: OracleDirectory | None = None,
    ):
        self._path = PurePath(file)
        self.connection = connection
        path_dir = PurePath(self._path.parent)

        if self.file_type is not FileType.UNKNOWN:
            if not self._path.suffix == self.file_type.suffix:
                raise UsageError("Unexpected file suffix: %s", self._path.suffix)

        if directory and str(path_dir) != ".":
            raise UsageError(
                "Cannot access sub-directory %s of an Oracle directory", path_dir
            )

        if directory:
            self._directory = directory
            if not self._directory.has_handler():
                self._directory.get_handler(connection)
        elif str(path_dir) != ".":
            self._directory = OracleDirectory.from_path(path_dir, connection)
        else:
            self._directory = OracleDirectory.default(connection)

        self.handler = OracleFileHandler(
            self.name, self._directory.name, self.connection
        )

    def __getattr__(self, attr) -> Any:
        return getattr(self.handler, attr)

    def __fspath__(self) -> str:
        return str(self._path)

    def __repr__(self) -> str:
        msg = ",".join(
            repr(f) for f in [str(self._path), self.directory, self.connection]
        )
        return f"{self.__class__.__name__}({msg})"

    def __eq__(self, other: OracleFile) -> bool:
        return (
            self._path == other._path
            and self.connection == other.connection
            and self.directory == self.directory
        )

    def __str__(self) -> str:
        return str(self.fullpath)

    def __hash__(self) -> int:
        return hash(self._path) + hash(self.connection) + hash(self.directory)

    @property
    def fullpath(self) -> PurePath:
        dir = self.directory.path if self.directory.has_handler else PurePath()
        return dir.joinpath(self.path)

    @property
    def name(self) -> str:
        return self._path.name

    @property
    def directory(self) -> OracleDirectory:
        return self._directory

    @property
    def exists(self) -> bool:
        return self.handler.file_exists

    @property
    def size(self) -> int:
        return self.handler.file_length

    @property
    def path(self) -> PurePath:
        return self.handler.path

    def get_file_handle(self, mode: SupportedOpenModes) -> OracleFileHandler:
        return self.handler.get_file_handle(mode)


def requires_file_handle(read: bool = True, write: bool = False):
    def decorator(_method: Callable):
        def wrapper(self, *args, **kwargs):
            if self.file_handle is None:
                raise ValueError(
                    "Method %s() requires file handle!"
                    "Call get_file_handle() first." % _method.__name__
                )

            if read and write:
                pass
            elif read and "r" not in self.mode:
                raise ValueError(
                    "Method %s() requires file open mode 'r'" % _method.__name__
                )
            elif write and "w" not in self.mode:
                raise ValueError(
                    "Method %s() requires file open mode 'w'" % _method.__name__
                )
            return _method(self, *args, **kwargs)

        return wrapper

    return decorator


class OracleFileHandler:
    def __init__(
        self,
        file_name: str,
        ora_directory: str,
        connection: Connection | Callable[..., Connection],
        mode: str | None = None,
    ):
        self.connection = resolve_connection(connection)

        self.ora_directory = ora_directory
        self._directory_handler = OracleDirectoryHandler(ora_directory, self.connection)
        self.file_name = file_name

        if mode:
            self.get_file_handle(mode)
        else:
            self.mode = None
            self.file_handle = None

    @property
    def path(self) -> PurePath:
        return self._directory_handler.path / PurePath(self.file_name)

    # TODO: consider caching
    @property
    def file_exists(self) -> bool:
        return self.get_attrs()[0]

    # TODO: consider caching
    @property
    def file_length(self) -> int:
        return self.get_attrs()[1]

    # TODO: consider caching
    @property
    def block_size(self) -> int:
        return self.get_attrs()[2]

    def get_file_handle(self, mode: str) -> DB_OBJECT:
        file_type: DB_OBJECT_TYPE = self.connection.gettype("UTL_FILE.FILE_TYPE")

        try:
            with self.connection.cursor() as cursor:
                self.mode = mode
                self.file_handle = cursor.callfunc(
                    name="UTL_FILE.FOPEN",
                    return_type=file_type,
                    keyword_parameters=dict(
                        location=self.ora_directory,
                        filename=self.file_name,
                        open_mode=self.mode,
                        max_linesize=constants.RAW_MAX_BYTES,
                    ),
                )
        except DatabaseError as exc:
            exc.add_note(f"file: {self.ora_directory}/{self.file_name}")
            raise exc

    def get_attrs(self):
        with self.connection.cursor() as cursor:
            file_exists = cursor.var(bool)
            file_length = cursor.var(int)
            block_size = cursor.var(int)
            cursor.callproc(
                name="UTL_FILE.FGETATTR",
                keyword_parameters=dict(
                    location=self.ora_directory,
                    filename=self.file_name,
                    fexists=file_exists,
                    file_length=file_length,
                    block_size=block_size,
                ),
            )
            return (
                cast(bool, file_exists.getvalue()),
                cast(int, file_length.getvalue()),
                cast(int, block_size.getvalue()),
            )

    def get_info(self) -> tuple[dict, int]:
        if not self.file_name.endswith(FileType.DUMP_FILE.suffix):
            raise UsageError("get_info() is only valid for dumpfiles!")

        info_obj_typ: DB_OBJECT_TYPE = self.connection.gettype("SYS.KU$_DUMPFILE_INFO")
        with self.connection.cursor() as cursor:
            info_obj = cursor.var(info_obj_typ)
            dmp_file_type = cursor.var(int)
            cursor.callproc(
                name="dbms_datapump.get_dumpfile_info",
                keyword_parameters={
                    "filename": self.file_name,
                    "directory": self.ora_directory,
                    "info_table": info_obj,
                    "filetype": dmp_file_type,
                },
            )
            dmp_file_type = cast(int, dmp_file_type.getvalue())
            info_data = cast(dict, object_loads(info_obj.getvalue()))  # type: ignore
            logger.debug(info_data)

        return info_data, dmp_file_type

    @requires_file_handle(read=True, write=False)
    def read(self, size=constants.RAW_MAX_BYTES) -> bytes:
        if size > constants.RAW_MAX_BYTES:
            raise ValueError(
                "Cannot request more than %d at a time", constants.RAW_MAX_BYTES
            )
        with self.connection.cursor() as cursor:
            buf = cursor.var(bytes, size=constants.RAW_MAX_BYTES)
            try:
                cursor.callproc(
                    name="UTL_FILE.GET_RAW",
                    keyword_parameters=dict(
                        file=self.file_handle, buffer=buf, len=size
                    ),
                )
            except DatabaseError as e:
                (error,) = e.args
                if error.code == sql.NO_DATA_FOUND:
                    return b""
                else:
                    raise e
            data = buf.getvalue()
            if data is None:
                data = b""
            return cast(bytes, data)

    @requires_file_handle(read=False, write=True)
    def write(self, chunk: bytes) -> int:
        if len(chunk) > constants.RAW_MAX_BYTES:
            raise ValueError(
                "Cannot write more than %d bytes in a single call",
                constants.RAW_MAX_BYTES,
            )
        with self.connection.cursor() as cursor:
            cursor.callproc(
                name="UTL_FILE.PUT_RAW",
                keyword_parameters=dict(
                    file=self.file_handle, buffer=chunk, autoflush=True
                ),
            )
            return len(chunk)

    def is_open(self) -> bool:
        with self.connection.cursor() as cursor:
            return cast(
                bool,
                cursor.callfunc(
                    name="UTL_FILE.IS_OPEN",
                    return_type=bool,
                    keyword_parameters=dict(file=self.file_handle),
                ),
            )

    def delete(self) -> int:
        self.get_attrs()
        if self.file_exists:
            with self.connection.cursor() as cursor:
                try:
                    cursor.callproc(
                        name="UTL_FILE.FREMOVE",
                        keyword_parameters=dict(
                            location=self.ora_directory, filename=self.file_name
                        ),
                    )
                except DatabaseError:
                    return 1
        return 0

    @requires_file_handle(read=True, write=True)
    def close(self) -> None:
        with self.connection.cursor() as cursor:
            if self.is_open():
                cursor.callproc(
                    name="UTL_FILE.FCLOSE",
                    keyword_parameters=dict(file=self.file_handle),
                )


class OracleFileReader(io.BufferedIOBase, IO[bytes]):
    def __init__(self, file_handler: OracleFileHandler):
        self._file_handler = file_handler
        self._buffer = bytearray()
        self._pos = 0
        self._eof = False
        self._line_terminator = b"\n"

    def read(self, size=-1, /) -> bytes:
        if size == 0:
            return b""
        if size < 0:
            while not self._eof:
                self._read_to_buffer()
            return self._consume_from_buffer(len(self._buffer))
        if self._eof:
            if len(self._buffer) == 0:
                return b""
            else:
                return self._consume_from_buffer(size)
        else:
            while not self._eof and len(self._buffer) < size + self._pos:
                self._read_to_buffer()
            return self._consume_from_buffer(size)

    def read1(self, size=-1, /) -> bytes:
        return self.read(size)

    def readinto(self, buf: bytearray | memoryview, /) -> int:
        data = self.read(len(buf))
        if not data:
            return 0
        buf[: len(data)] = data
        return len(data)

    def readinto1(self, buf: bytearray | memoryview, /) -> int:
        return self.readinto(buf)

    def readline(self, limit=-1, /) -> bytes:
        if limit != -1:
            raise NotImplementedError("limit parameter not implemented!")

        while (
            not (term_pos := self._search_buffer(self._line_terminator))
            and not self._eof
        ):
            self._read_to_buffer()

        if term_pos is not None:
            size = term_pos + 1
        else:
            size = -1

        return self._consume_from_buffer(size)

    __closed = False

    def close(self):
        if not self.__closed:
            try:
                logger.debug("Closing %s", self._file_handler.file_name)
                if self._file_handler.is_open:
                    self._file_handler.close()

                del self._buffer
            finally:
                self.__closed = True

    def __del__(self):
        logger.debug("Destructing %s file stream", self._file_handler.file_name)
        try:
            self.close()
        except Exception as e:
            logger.warning(
                "Unable to close file %s. Caught: %s", self._file_handler.file_name, e
            )

    def seekable(self) -> bool:
        return False

    def readable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def _search_buffer(self, val: bytes) -> int | None:
        try:
            return self._buffer.index(val)
        except ValueError:
            return None

    def _consume_from_buffer(self, size=-1) -> bytes:
        if size == -1:
            chunk = slice(0, None)
        else:
            chunk = slice(self._pos, min(self._pos + size, len(self._buffer)))
        try:
            return self._buffer[chunk]
        finally:
            del self._buffer[chunk]

    def _read_to_buffer(self, size: int = constants.RAW_MAX_BYTES) -> None:
        if not self._eof:
            data = self._file_handler.read(size)
            if len(data) < size:
                self._eof = True
            self._buffer += data


class OracleFileWriter(io.BufferedIOBase, IO[bytes]):
    MAX_BUFFER = 1024 * 1024

    def __init__(self, file_handler: OracleFileHandler):
        self._file_handler = file_handler
        self._bytes_written = 0

    def write(self, buf: bytes) -> int:
        bufmv = memoryview(buf)

        for i in range(-(len(bufmv) // -constants.RAW_MAX_BYTES)):
            chunk = slice(
                i * constants.RAW_MAX_BYTES,
                min(len(bufmv), (i + 1) * constants.RAW_MAX_BYTES),
            )
            logger.debug(
                "Writing %d bytes to %s",
                len(bufmv[chunk]),
                self._file_handler.file_name,
            )
            self._file_handler.write(bufmv[chunk].tobytes())
            self._bytes_written += len(bufmv[chunk])

        return len(buf)

    def flush(self) -> None:
        pass

    def writable(self) -> bool:
        return True

    __closed = False

    def close(self) -> None:
        logger.debug("close() called %s", self._file_handler.file_name)
        if not self.__closed:
            try:
                logger.debug("Closing %s", self._file_handler.file_name)
                self.flush()

                if self._file_handler.is_open:
                    self._file_handler.close()
            finally:
                self.__closed = True

    def __del__(self) -> None:
        logger.debug("Destructing %s file stream", self._file_handler.file_name)
        try:
            self.close()
        except Exception as e:
            logger.warning(
                "Unable to close file %s. Caught: %s", self._file_handler.file_name, e
            )

    def tell(self):
        return self._bytes_written

    def detach(self):
        raise io.UnsupportedOperation("detach() not supported")


def parse_open_mode(
    mode: SupportedOpenModes,
) -> tuple[constants.ContentType, constants.IOMode]:
    content_type: constants.ContentType = str
    if "b" in mode:
        content_type = bytes
        if "t" in mode:
            raise io.UnsupportedOperation("Conflicting io modes %s", mode)

    io_mode: constants.IOMode = "r"
    if "w" in mode:
        io_mode = "w"
        if "r" in mode:
            raise io.UnsupportedOperation("Read/write mode not supported %s", mode)

    return content_type, io_mode


@overload
def ora_open(
    file: OracleFile, /, mode: SupportedOpenModes = "r", encoding: str | None = None
) -> IO:
    ...


@overload
def ora_open(
    file: OracleFileHandler,
    /,
    mode: SupportedOpenModes = "r",
    encoding: str | None = None,
) -> IO:
    ...


@overload
def ora_open(
    *,
    ora_directory: str,
    file_name: str,
    database_ctx: Mapping[str, object] | None = None,
    connection: Connection | Callable[..., Connection],
    mode: SupportedOpenModes = "r",
    encoding: str | None = None,
) -> IO:
    ...


def ora_open(
    file: OracleFile | OracleFileHandler | None = None,
    /,
    mode: SupportedOpenModes = "r",
    encoding: str | None = None,
    ora_directory: str | None = None,
    file_name: str | None = None,
    connection: Connection | Callable[..., Connection] | None = None,
    **kwargs,
) -> IO:

    if file and (file_name or ora_directory):
        raise UsageError(
            "Provide a positional file arg or (ora_directory and file_name) arg not "
            "both!"
        )

    if file_name and ora_directory and connection:
        if callable(connection):
            connection_provider = connection
        else:
            connection_provider = lambda: connection

        file_handler = OracleFileHandler(
            ora_directory, file_name, connection_provider, mode
        )
    elif isinstance(file, OracleFile):
        file.get_file_handle(mode)
        file_handler = file.handler
    elif isinstance(file, OracleFileHandler):
        file_handler = file
    else:
        raise NotImplementedError(
            "Cannot open type %s or not all file args provided "
            "(ora_directory=%s ,file_name=%s, connection=%s)",
            file,
            ora_directory,
            file_name,
            connection,
        )

    content_type, io_mode = parse_open_mode(mode)

    # Binary modes
    if io_mode == "r" and content_type is bytes and encoding is None:
        return OracleFileReader(file_handler)
    if io_mode == "w" and content_type is bytes and encoding is None:
        return OracleFileWriter(file_handler)

    # Text modes
    text_encoding = io.text_encoding(encoding)  # type: ignore

    if io_mode == "r" and (content_type is str or encoding):
        return io.TextIOWrapper(OracleFileReader(file_handler), encoding=text_encoding)
    if io_mode == "w" and (content_type is str or encoding):
        return io.TextIOWrapper(
            OracleFileWriter(file_handler), encoding=text_encoding, write_through=True
        )
    else:
        raise NotImplementedError(mode)


class DumpFile(OracleFile):
    """Datapump dumpfile"""

    file_type = FileType.DUMP_FILE

    def get_info(self) -> tuple[dict, int]:
        info_data, dmp_file_type = self.handler.get_info()
        return info_data, dmp_file_type


UriStr: TypeAlias = str
DumpFileTyp: TypeAlias = DumpFile | str | PurePath | UriStr


class SubstitutionVarMixin:
    HAS_SEQ_RE: ClassVar[re.Pattern[str]] = re.compile(
        r".*" + re.escape(constants.SUB_SEQ_VAR) + r".*"
    )

    def has_sequence_var(self, path: PathLike[str]) -> bool:
        return self.HAS_SEQ_RE.match(os.fspath(path)) is not None

    def with_sequence_var(self, path: PurePath) -> PurePath:
        return path.with_stem(path.stem + constants.NAME_DELIM + constants.SUB_SEQ_VAR)

    def ensure_dumpfile_count(
        self,
        files: list[DumpFile],
        file_set_ident: str,
        build_subst_dumpfile: Callable[..., DumpFile],
        num_files_required: int,
    ) -> list[DumpFile]:

        if num_files_required != len(files) and not any(
            self.has_sequence_var(file) for file in files
        ):
            subst_dumpfile = build_subst_dumpfile(file_set_ident)
            files.append(subst_dumpfile)

        return files

    def resolve_sequences_for_import(
        self,
        files: list[DumpFile],
        build_seq_dumpfile: Callable[[str, int], DumpFile],
        num_files_required: int,
    ) -> set[DumpFile]:
        unresolved_files: set[DumpFile] = set()
        resolved_files: set[DumpFile] = set()

        logger.debug("files: %s", files)
        logger.debug("num_files_required: %s", num_files_required)

        while files:
            file = files.pop()
            if self.has_sequence_var(file):
                unresolved_files.add(file)
            else:
                resolved_files.add(file)

        for file in resolved_files:
            if not file.exists:
                raise UsageError("Oracle file %s not found on sever!" % file.path)

        # Discover dump files on the db server that match sequence masks.
        # Oracle doesn't allow listing of the directory so we have to
        # brute force try/catch the possible names stopping once the
        # next sequence is not found.

        for file in unresolved_files:
            seq_candidate = 1
            while True:
                candidate = build_seq_dumpfile(file.name, seq_candidate)
                if candidate.exists:
                    resolved_files.add(candidate)
                    seq_candidate += 1
                else:
                    break

        if len(resolved_files) != num_files_required:
            logger.warning(
                "Parallelism setting of %d does not match the number of dumpfiles: %s",
                num_files_required,
                resolved_files,
            )

        return resolved_files

    def resolve_sequences_for_export(
        self,
        files: list[DumpFile],
        make_seq_dumpfile: Callable[[str, int], DumpFile],
        num_files_required: int,
    ) -> set[DumpFile]:
        unresolved_files: set[DumpFile] = set()
        resolved_files: set[DumpFile] = set()

        logger.debug("files: %s", files)
        logger.debug("num_files_required: %s", num_files_required)

        while files:
            file = files.pop()
            if self.has_sequence_var(file):
                unresolved_files.add(file)
            else:
                resolved_files.add(file)

        for file in unresolved_files:
            for seq_num in range(1, (num_files_required // len(unresolved_files) + 1)):
                sequenced_file = make_seq_dumpfile(file.name, seq_num)
                resolved_files.add(sequenced_file)

        return resolved_files

    def resolve_sequences(
        self,
        operation: Operation,
        files: list[DumpFile],
        make_seq_dumpfile: Callable[[str, int], DumpFile],
        num_files_required: int,
    ) -> set[DumpFile]:

        if operation is Operation.IMPORT:
            resolver = self.resolve_sequences_for_import
        elif operation is Operation.EXPORT:
            resolver = self.resolve_sequences_for_export
        elif operation is Operation.SQL_FILE:
            raise NotImplementedError(operation)
        else:
            assert_never(operation)

        return resolver(files, make_seq_dumpfile, num_files_required)


class DumpFileHandler(SubstitutionVarMixin):
    def __init__(
        self,
        operation: Operation,
        files: list[DumpFile | str | PurePath | UriStr],
        database_ctx: Connection,
        file_set_ident: str,
        num_files_required: int,
    ):
        self.operation = operation
        self.files = files
        self.database_ctx = database_ctx
        self.file_set_ident = file_set_ident
        self.num_files_required = num_files_required

        self._directory_freq = collections.Counter()

    def prepare(self) -> set[DumpFile]:
        unresolved_files: Sequence[DumpFile] = [
            self.coerce(file) for file in self.files
        ]

        for f in unresolved_files:
            self._directory_freq[f.directory] += 1

        unresolved_files = self.ensure_dumpfile_count(
            files=unresolved_files,
            file_set_ident=self.file_set_ident,
            build_subst_dumpfile=self.build_subst_dumpfile,
            num_files_required=self.num_files_required,
        )
        return self.resolve_sequences(
            operation=self.operation,
            files=unresolved_files,
            make_seq_dumpfile=self.build_seq_dumpfile,
            num_files_required=self.num_files_required,
        )

    def build_subst_dumpfile(self, file_set_ident: str | None = None) -> DumpFile:
        common_dir = self._most_common_dir()
        file_set_ident = file_set_ident if file_set_ident else self.file_set_ident
        file = file_set_ident + "_" + constants.SUB_SEQ_VAR + FileType.DUMP_FILE.suffix
        return DumpFile(
            file=file,
            connection=self.database_ctx,
            directory=common_dir or OracleDirectory.default(self.database_ctx),
        )

    def build_seq_dumpfile(self, file_name: str, seq_num: int) -> DumpFile:
        common_dir = self._most_common_dir()
        new_name = re.sub(constants.SUB_SEQ_VAR, str(seq_num).zfill(2), str(file_name))
        return DumpFile(
            file=new_name,
            connection=self.database_ctx,
            directory=common_dir or OracleDirectory.default(self.database_ctx),
        )

    def _most_common_dir(self) -> OracleDirectory | None:
        if len(self._directory_freq) > 0:
            return self._directory_freq.most_common(1)[0][0]
        else:
            return None

    def coerce(self, file) -> DumpFile:
        match file:
            case DumpFile():
                return file
            case builtins.str() | PurePath():
                return DumpFile(file=file, connection=self.database_ctx)
            case _:
                raise NotImplementedError(
                    f"Cannot coerce type {type(file)} to DumpFile!"
                )


class DumpFileSet:
    """
    Constructs a set of one or more dump files to be used in
    a datapump job. File name can be a literal file names or
    a mask name representing more than one dump file using
    the following substitution variables:

    %U - Expands to a numerical sequence [01,02,...]. Used
         for parallel export/import with multiple files.
    %D - Expands to Gregorian day DD. Export only.
    %M - Expands to Gregorian month MM. Export only.
    %Y - Expands Gregorian year YYYY. Export only.
    %T - Expands Gregorian date YYYYMMDD. Export only.

    If the number of dump files provided is less than the
    parallel degree setting for the job the last file name
    will be modified to include "_%U.dmp". If no dump files
    are provided to an export job then system generated files
    will be used.
    """

    file_type = FileType.DUMP_FILE_SET

    def __init__(self, *files: DumpFile | str | PurePath):
        self.unprepared_files = list(files)
        self.prepared_files: set[DumpFile] = set()

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"{','.join(repr(f) for f in self.prepared_files)})"
        )

    def __iter__(self) -> Iterator[DumpFile]:
        return iter(self.dumpfiles)

    def add(self, file: DumpFile | str | PurePath):
        self.unprepared_files.append(file)

    @property
    def prepared(self) -> bool:
        return self.prepared_files is not None

    @property
    def dumpfiles(self) -> set[DumpFile]:
        return self.prepared_files

    @classmethod
    def from_running_job(
        cls, job_dumpfiles: Iterable, connection: Connection
    ) -> Self:
        if job_dumpfiles:
            return cls(*(DumpFile(df.path, connection) for df in job_dumpfiles))
        raise RuntimeError("job_dumpfiles not found!")

    def prepare(
        self,
        connection: Connection,
        file_set_ident: str,
        operation: Operation,
        parallel: int,
    ) -> None:
        logger.info(
            "Preparing DumpFileSet: %s",
            list(map(str, self.unprepared_files)),
            ctx=connection,
        )

        handler = DumpFileHandler(
            operation=operation,
            files=self.unprepared_files,
            database_ctx=connection,
            file_set_ident=file_set_ident,
            num_files_required=parallel,
        )

        self.prepared_files = handler.prepare()

        logger.info("prepared DumpFileSet: %s", list(map(str, self.prepared_files)))

    def get_full_paths(self) -> list[PurePath]:
        return [file.path for file in self.dumpfiles]


class LogFile(OracleFile):
    file_type = FileType.LOG_FILE


_T = TypeVar("_T", str, bytes)


class LobStreamer(Generic[_T]):
    def __init__(self, lob: DB_LOB):
        self._lob = lob
        self._content_type: type[str] | type[bytes] = (
            str if self._lob.type in ("DB_TYPE_CLOB", "CLOB") else bytes
        )
        self._default_fetch_size = lob.getchunksize() * constants.LOB_FETCH_SIZE_MULT
        self._empty = (
            constants.EMPTY_STR if self._content_type is str else constants.EMPTY_BYTE
        )
        self._pos = 1
        self._complete = False

    def read(self, size: int = -1, /) -> _T | None:
        if size == -1:
            data = self._empty

            while not self._complete:
                chunk = self._fetch(self._default_fetch_size)

                if chunk is not None and isinstance(chunk, self._content_type):
                    data += chunk  # type: ignore
            return data  # type: ignore
        else:
            return self._fetch(size)

    def _fetch(self, size: int) -> _T | None:
        if self._complete:
            return None

        chunk = self._lob.read(self._pos, size)

        if len(chunk) < size:
            self._complete = True

        self._pos += len(chunk)
        return chunk  # type: ignore


class LobReader(io.RawIOBase):
    def __init__(self, lob: DB_CLOB) -> None:
        self._stream = LobStreamer[bytes](lob)

    def read(self, size: int = -1, /) -> bytes | None:
        return self._stream.read(size)

    def readall(self):
        return self._stream.read()

    def readinto(self, buf: bytearray) -> int:
        buf_size = len(buf)
        mv_buf = memoryview(buf)
        data = self.read()
        if data and len(data) > 0:
            mv_buf[: len(data)] = data
            return len(buf) - buf_size
        else:
            return 0

    def readable(self) -> bool:
        return True


class OracleLOBWrapper(IO):
    """
    Provides a file-like interface for Oracle LOB. Callers should use the open
    classmethod
    """

    chunk_size_mult = 8

    def __init__(self):
        self._pos = 1

        # Set by open()
        self._lob: DB_LOB
        self._content_type: constants.ContentType
        self._io_mode: constants.IOMode
        self._encoding: str | None
        self._default_chunk_size: int
        self._chunk_size: int
        self._raw_stream: Iterator[bytes] | None = None
        self._decoded_stream: Iterator[str] | None = None

    def _stream_chunks(self, chunk_size: int | None = None) -> Iterator[bytes]:
        if chunk_size and chunk_size > 0:
            init_chunk_size = chunk_size
        else:
            init_chunk_size = self._default_chunk_size

        self._chunk_size = init_chunk_size
        while True:
            chunk_size = self._chunk_size
            data = cast(bytes, self._lob.read(self._pos, chunk_size))
            if data:
                yield data
            if len(data) < chunk_size:
                break
            self._pos += len(data)

    def _stream_decode(self, stream: Iterable[bytes], encoding: str) -> Iterator[str]:
        yield from iterdecode(stream, encoding)

    @property
    def content_type(self) -> constants.ContentType:
        return self._content_type

    @classmethod
    def open(
        cls,
        lob: DB_LOB,
        /,
        mode: SupportedOpenModes = "r",
        encoding: str | None = None,
    ) -> IO:
        instance = cls()
        instance._lob = lob
        instance._content_type, instance._io_mode = parse_open_mode(mode)
        instance._encoding = encoding
        instance._default_chunk_size = lob.getchunksize() * cls.chunk_size_mult
        instance._chunk_size = instance._default_chunk_size
        return instance

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_ty, exec_val, tb) -> None:
        if self._io_mode == "w" and self._lob.isopen():
            self._lob.close()

    def read(self, size: int = -1) -> str | bytes | None:
        if not self._raw_stream:
            self._raw_stream = self._stream_chunks(size)

        if not self._decoded_stream and self._encoding:
            self._decoded_stream = self._stream_decode(self._raw_stream, self._encoding)

        if self._decoded_stream and self._encoding:
            if size == -1:
                data = str()
                for chunk in self._decoded_stream:
                    data += chunk
                return data
            else:
                next(self._decoded_stream)
        else:
            if size == -1:
                data = bytes() if self.content_type == bytes else str()
                for chunk in self._raw_stream:
                    data += chunk  # type: ignore
                    return data
            else:
                self._chunk_size = size
                return next(self._raw_stream)

    def readline(self) -> str | bytes | None:
        raise NotImplementedError()

    def write(self, data) -> None:
        raise NotImplementedError()

    def seek(self, position):
        raise NotImplementedError()

    def tell(self) -> int:
        return self._pos
