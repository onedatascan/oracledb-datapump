# oracledb-datapump
oracledb-datapump is a Python package for running [Oracle Datapump](https://docs.oracle.com/en/database/oracle/oracle-database/21/sutil/oracle-data-pump.html#GUID-501A9908-BCC5-434C-8853-9A6096766B5A) remotely and without the need to install an Oracle Database client.


# Quick Start
There are three primary modes of usage:
* Python package
* CLI
* AWS Lambda

## Python package
### Export (synchronous)
```python
import logging

from oracledb_datapump import Job, Directive, Operation, JobMode

logging.basicConfig(level="INFO")


job = Job(
    operation=Operation.EXPORT,
    mode=JobMode.SCHEMA,
    directives=[Directive.INCLUDE_SCHEMA("HR"), Directive.PARALLEL(2)],
)
response = job.run(wait=True, connection="system/manager@localhost/orclpdb1")
print(response)
print(job.get_logfile())
```

### Export (asynchronous)
```python
import logging

from oracledb_datapump import Job, Directive, Operation, JobMode

logging.basicConfig(level="INFO")


job = Job(
    operation=Operation.EXPORT,
    mode=JobMode.SCHEMA,
    directives=[Directive.INCLUDE_SCHEMA("HR"), Directive.PARALLEL(2)],
)
response = job.run(
    wait=False, connection="system/manager@localhost/orclpdb1"
)
print(response)

result = job.poll_for_completion(30)
print(result)
print(job.get_logfile())
```

### Import (synchronous)
```python
import logging

from oracledb_datapump import Job, Directive, Operation, JobMode

logging.basicConfig(level="INFO")


job = Job(
    operation=Operation.IMPORT,
    mode=JobMode.SCHEMA,
    dumpfiles=["EXP-HR-20230316210554292374_%U.dmp"],
    directives=[
        Directive.INCLUDE_SCHEMA("HR"),
        Directive.PARALLEL(2),
        Directive.REMAP_SCHEMA(old_value="HR", value="HR2")
    ],
)
response = job.run(
    wait=True, connection="system/manager@localhost/orclpdb1"
)
print(response)
print(job.get_logfile())
```

### Status on existing job
```python
import logging

from oracledb_datapump import Job

logging.basicConfig(level="INFO")


job = Job.attach(
    connection="system/manager@localhost/orclpdb1",
    job_name="EXP-HR-20230320145316600771",
    job_owner="SYSTEM"
)
print(job.get_status())
print(job.get_logfile())

```

### Export using JSON request (synchronous)
```python
import logging
import json

from oracledb_datapump.client import DataPump

logging.basicConfig(level="INFO")


job_request = {
    "connection": {
        "user": "system",
        "password": "manager",
        "host": "localhost",
        "database": "orclpdb1"
    },
    "request": "SUBMIT",
    "payload": {
        "operation": "EXPORT",
        "mode": "SCHEMA",
        "wait": True,
        "directives": [
            {"name": "INCLUDE_SCHEMA", "value": "HR"}
        ]
    }
}

response = DataPump.submit(json.dumps(job_request))
print(response)

logfile = DataPump.get_logfile(
    str(response.logfile),
    connection={
        "user": "system",
        "password": "manager",
        "host": "localhost",
        "database": "orclpdb1"
    }
)
print(logfile)
```

### Import using JSON request (synchronous)
```python
import logging
import json

from oracledb_datapump.client import DataPump

logging.basicConfig(level="INFO")


job_request = {
    "connection": {
        "user": "system",
        "password": "manager",
        "host": "localhost",
        "database": "orclpdb1",
    },
    "request": "SUBMIT",
    "payload": {
        "operation": "IMPORT",
        "mode": "SCHEMA",
        "wait": True,
        "dumpfiles": ["HR.dmp"],
        "directives": [
            {"name": "PARALLEL", "value": 1},
            {"name": "INCLUDE_SCHEMA", "value": "HR"},
            {"name": "OID", "value": False},
            {"name": "REMAP_SCHEMA", "old_value": "HR", "value": "HR2"},
            {"name": "TABLE_EXISTS_ACTION", "value": "REPLACE"},
        ],
    },
}

response = DataPump.submit(json.dumps(job_request))
print(response)

logfile = DataPump.get_logfile(
    str(response.logfile),
    connection={
        "user": "system",
        "password": "manager",
        "host": "localhost",
        "database": "orclpdb1"
    }
)
print(logfile)
```

### Import using JSON request (asynchronous w/ polling)
```python
import logging
import json
from time import sleep

from oracledb_datapump.client import DataPump

logging.basicConfig(level="INFO")


job_request = {
    "connection": {
        "user": "system",
        "password": "manager",
        "host": "localhost",
        "database": "orclpdb1",
    },
    "request": "SUBMIT",
    "payload": {
        "operation": "IMPORT",
        "mode": "SCHEMA",
        "wait": False,
        "dumpfiles": ["HR.dmp"],
        "directives": [
            {"name": "PARALLEL", "value": 1},
            {"name": "INCLUDE_SCHEMA", "value": "HR"},
            {"name": "OID", "value": False},
            {"name": "REMAP_SCHEMA", "old_value": "HR", "value": "HR2"},
            {"name": "TABLE_EXISTS_ACTION", "value": "REPLACE"},
        ],
    },
}

response = DataPump.submit(json.dumps(job_request))
print(response)

status_request = {
    "connection": {
        "user": "system",
        "password": "manager",
        "host": "localhost",
        "database": "orclpdb1",
    },
    "request": "STATUS",
    "payload": {
        "job_name": response.job_name,
        "job_owner": response.job_owner,
        "type": "LOG_STATUS",
    },
}

status = DataPump.submit(json.dumps(status_request))
while status.state not in ("COMPLETED", "STOPPED"):
    print(status)
    sleep(15)
    status = DataPump.submit(json.dumps(status_request))

print(f"final status: {status}")
```

## CLI
```bash
$ oracledb-datapump --help
usage: oracledb-datapump [-h] (--schema SCHEMA | --full | --table TABLE) --user USER --password PASSWORD --host HOST --database DATABASE [--parallel PARALLEL] [--dumpfile DUMPFILE]
                         [--compression {DATA_ONLY,METADATA_ONLY,ALL,NONE}] [--exclude EXCLUDE] [--remap_schema REMAP_SCHEMA] [--remap_tablespace REMAP_TABLESPACE]
                         [--flashback_utc FLASHBACK_UTC] [--directive DIRECTIVE]
                         {import,export,impdp,expdp}

Remote Oracle Datapump (limited feature set)

positional arguments:
  {import,export,impdp,expdp}

options:
  -h, --help            show this help message and exit
  --schema SCHEMA
  --full
  --table TABLE
  --user USER           Oracle admin user
  --password PASSWORD   Oracle admin password
  --host HOST           Database service host
  --database DATABASE   Database service name
  --parallel PARALLEL   Number of datapump workers
  --dumpfile DUMPFILE   Oracle dumpfile - Required for import
  --compression {DATA_ONLY,METADATA_ONLY,ALL,NONE}
  --exclude EXCLUDE     Exclude object type
  --remap_schema REMAP_SCHEMA
                        Remap schema FROM_SCHEMA:TO_SCHEMA
  --remap_tablespace REMAP_TABLESPACE
                        Remap tablespace FROM_TBLSPC:TO_TBLSPC
  --flashback_utc FLASHBACK_UTC
                        ISO format UTC timestamp
  --directive DIRECTIVE
                        Datapump directive NAME:VALUE
```

### Export
```bash
oracledb-datapump --user system --password manager --host localhost --database orclpdb1 --parallel 2 --schema hr export
```

### Import
```bash
oracledb-datapump --user system --password manager --host localhost --database orclpdb1 --schema HR --dumpfile HR.dmp --remap_schema "HR:HR2" import
```

## HTTP Server as AWS Lambda
This example assumes the use of a custom domain name mapped to an API Gateway or ALB where the datapump lambda is mapped to a `datapump` endpoint.

### Export
```bash
curl -XPOST "https://oracledb-util-api.somedomain.com/datapump" -d \
 '{
    "connection": {
        "user": "system",
        "password": "manager",
        "host": "host.docker.internal",
        "database": "orclpdb1"
    },
    "request": "SUBMIT",
    "payload": {
        "operation": "EXPORT",
        "mode": "SCHEMA",
        "wait": false,
        "directives": [
            {"name": "PARALLEL", "value": 2},
            {"name": "COMPRESSION", "value": "ALL"},
            {"name": "INCLUDE_SCHEMA", "value": "HR"}
        ]
    }
}'
```

### Import
```bash
response=$(curl -XPOST "https://oracledb-util-api.somedomain.com/datapump" -d \
 '{
    "connection": {
        "user": "system",
        "password": "manager",
        "host": "host.docker.internal",
        "database": "orclpdb1"
    },
    "request": "SUBMIT",
    "payload": {
        "operation": "IMPORT",
        "mode": "SCHEMA",
        "wait": false,
        "dumpfiles": ["HR.dmp"],
        "directives": [
            {"name": "PARALLEL", "value": 2},
            {"name": "INCLUDE_SCHEMA", "value": "HR"},
            {"name": "OID", "value": false},
            {"name": "REMAP_SCHEMA", "old_value": "HR", "value": "HR2"},
            {"name": "TABLE_EXISTS_ACTION", "value": "REPLACE"}
        ]
    }
}'| jq '.body | fromjson')
echo $response
JOB_NAME=$(jq -r '.job_name' <<< $response)
JOB_OWNER=$(jq -r '.job_owner' <<< $response)
```

### Status
```bash
curl -XPOST "https://oracledb-util-api.somedomain.com/datapump" -d \
 '{
    "connection": {
        "user": "system",
        "password": "manager",
        "host": "host.docker.internal",
        "database": "orclpdb1"
    },
    "request": "STATUS",
    "payload": {
        "job_name": "'"$JOB_NAME"'",
        "job_owner": "'"$JOB_OWNER"'",
    }
}' | jq '.body | fromjson'
```

## Directives
Directives are used to set parameters, remaps and transforms on a Datapump job. Most of these map back to:
* [DBMS_DATAPUMP.METADATA_FILTER](https://docs.oracle.com/en/database/oracle/oracle-database/21/arpls/DBMS_DATAPUMP.html#GUID-84C5942C-B4CC-4128-8D7E-21EAC158F363)
* [DBMS_DATAPUMP.METADATA_REMAP](https://docs.oracle.com/en/database/oracle/oracle-database/21/arpls/DBMS_DATAPUMP.html#GUID-0FC32790-91E6-4781-87A3-229DE024CB3D)
* [DBMS_DATAPUMP.METADATA_TRANSFORM](https://docs.oracle.com/en/database/oracle/oracle-database/21/arpls/DBMS_DATAPUMP.html#GUID-DE64EDF1-5AD1-44B6-8B3A-8A3954096F71)
* [DBMS_DATAPUMP.SET_PARAMETER](https://docs.oracle.com/en/database/oracle/oracle-database/21/arpls/DBMS_DATAPUMP.html#GUID-62324358-2F26-4A94-B69F-1075D53FA96D)

Further information on each of these can be obtained from the Oracle documentation for your database version. Be aware that the usage of some of these parameters require feature based Oracle licenses.


The following is a list of valid directives:
```
EXCLUDE_OBJECT_TYPE - args: value: str, object_path: str | None
INCLUDE_SCHEMA - args: value: str, object_path: str | None
INCLUDE_TABLE - args: value: str, object_path: str | None
CLIENT_COMMAND - args: value: str
COMPRESSION - args: value: {DATA_ONLY, METADATA_ONLY, ALL, NONE}
COMPRESSION_ALGORITHM - args: value: {BASIC, LOW, MEDIUM, HIGH}
DATA_ACCESS_METHOD - args: value: {AUTOMATIC, DIRECT_PATH, EXTERNAL_TABLE}
DATA_OPTIONS - args: value: [SKIP_CONST_ERR, XMLTYPE_CLOB, NO_TYPE_EVOL, DISABL_APPEND_HINT, REJECT_ROWS_REPCHR, ENABLE_NET_COMP, GRP_PART_TAB, TRUST_EXIST_TB_PAR, VALIDATE_TBL_DATA, VERIFY_STREAM_FORM, CONT_LD_ON_FMT_ERR]
ENCRYPTION - args: value: {ALL, DATA_ONLY, ENCRYPTED_COLUMNS_ONLY, METADATA_ONLY}
ENCRYPTION_ALGORITHM - args: value: {AES128, AES192, AES256}
ENCRYPTION_MODE - args: value: {PASSWORD, TRANSPARENT, DUAL}
ENCRYPTION_PASSWORD - args: value: {PASSWORD, DUAL}
ESTIMATE - args: value: {BLOCKS, STATISTICS}
ESTIMATE_ONLY - args: value: int
FLASHBACK_SCN - args: value: int
FLASHBACK_TIME - args: value: str | datetime # Must be UTC ISO format.
INCLUDE_METADATA - args: value: bool
KEEP_MASTER - args: value: bool
LOGTIME - args: value: {NONE, STATUS, LOGFILE, ALL}
MASTER_ONLY - args: value: bool
METRICS - args: value: bool
PARTITION_OPTIONS - args: value: {NONE, DEPARTITION, MERGE}
REUSE_DATAFILES - args: value: bool
SKIP_UNUSABLE_INDEXES - args: value: bool
SOURCE_EDITION - args: value: bool
STREAMS_CONFIGURATION - args: value: bool
TABLE_EXISTS_ACTION - args: value: {TRUNCATE, REPLACE, APPEND, SKIP}
TABLESPACE_DATAFILE - args: value: {TABLESPACE_DATAFILE}
TARGET_EDITION - args: value: str
TRANSPORTABLE - args: value: {ALWAYS, NEVER}
TTS_FULL_CHECK - args: value: bool
USER_METADATA - args: value: bool
PARALLEL - args: value: int
REMAP_SCHEMA - args: old_value: str, value: str, object_path: str | None
REMAP_TABLESPACE - args: old_value: str, value: str, object_path: str | None
REMAP_DATAFILE - args: old_value: str, value: str, object_path: str | None
DISABLE_ARCHIVE_LOGGING - args: value: bool, object_path: str | None
INMEMORY - args: value: bool, object_path: str | None
INMEMORY_CLAUSE - args: value: str, object_path: str | None
LOB_STORAGE - args: value: {SECUREFILE, BASICFILE, DEFAULT, NO_CHANGE}, object_path: str | None
OID - args: value: bool, object_path: str | None
PCTSPACE - args: value: int, object_path: str | None
SEGMENT_ATTRIBUTES - args: value: bool, object_path: str | None
SEGMENT_CREATION - args: value: bool, object_path: str | None
STORAGE - args: value: bool, object_path: str | None
TABLE_COMPRESSION_CLAUSE - args: value: str | object_path: None
DELETE_FILES: Custom directive that deletes the dumpfiles once an import is complete. Valid only for synchronous executions.
```