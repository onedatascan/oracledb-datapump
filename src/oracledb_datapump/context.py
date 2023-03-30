from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, TypeVar, runtime_checkable

from oracledb_datapump.base import DataPumpHandle, JobMode, Operation
from oracledb_datapump.database import Connection


@runtime_checkable
class IJobContext(Protocol):
    job_name: str
    job_owner: str


@dataclass
class JobContext(IJobContext):
    connection: Connection
    job_name: str
    job_owner: str
    job_handle: DataPumpHandle


@dataclass
class OpenContext(JobContext):
    operation: Operation
    mode: JobMode
    parallel: int


@dataclass
class AttachContext(JobContext):
    ...


@dataclass
class StatusContext(IJobContext):
    connection: Connection
    job_name: str
    job_owner: str


CTX = TypeVar("CTX", bound=IJobContext, covariant=True)
