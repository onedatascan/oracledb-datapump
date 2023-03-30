SUCCESS_WITH_INFO = "31627"
INTERNAL_ERROR = "39006"
INVALID_ARGUMENT = "39001"
INVALID_OPERATION = "39002"
NO_DATA_FOUND = "01403"

SQL_GET_SCHEMA_TABLESPACES = """
    select distinct tablespace_name
    from dba_segments
    where owner = upper(:schema)
"""

SQL_GET_USERNAME = """
    select username
    from all_users
    where username = upper(:username)
"""

SQL_GET_EXPORT_OBJS = """
    select *
    from (
        select d.*, 'DATABASE_EXPORT_OBJECTS' as lookup_table
        from database_export_objects d
        union
        select s.*, 'SCHEMA_EXPORT_OBJECTS' as lookup_table
        from schema_export_objects s
        union
        select t.*, 'TABLE_EXPORT_OBJECTS' as lookup_table
        from table_export_objects t
        union
        select ts.*, 'TABLESPACE_EXPORT_OBJECTS' as lookup_table
        from tablespace_export_objects ts
    )
        where named is not null
        and lookup_table = :lookup_table
        order by lookup_table, 1
"""

SQL_GET_DIRECTORY_PATH = """
    select directory_path
    from all_directories
    where directory_name = upper(:dir_name)
"""

SQL_GET_DIRECTORY_FROM_PATH = """
    select directory_name
    from all_directories
    where rtrim(directory_path, '/') = :dir_path
"""

SQL_GET_DATAPUMP_JOB = """
    select owner_name,
           job_name,
           operation,
           job_mode,
           state
    from dba_datapump_jobs
    where owner_name = upper(:job_owner)
    and job_name = upper(:job_name)
"""

SQL_VALIDATE_OBJECT_NAME = """
    select sys.dbms_assert.qualified_sql_name(:schema || '.' || :object_name)
    from dual
"""

SQL_TABLE_EXISTS = """
    select table_name
    from dba_tables
    where owner = upper(:schema)
    and table_name = :table_name
"""

SQL_GET_DB_CHAR_ENCODING = """
    select utl_i18n.map_charset(value) as encoding,
           value as db_charset
    from nls_database_parameters
    where parameter = 'NLS_CHARACTERSET'
"""

SQL_GET_FLYWAY_SCHEMA_VERSION = """
    select *
    from {}
    order by 1 desc
    fetch first 1 rows only
"""

PLSQL_DROP_ORPHAN = """
    -- drop master table for "orphaned job" if it exists
    begin
        for i in ( select 'DROP TABLE '||owner_name||'.'||job_name||' PURGE' stat
                    from dba_datapump_jobs
                    where owner_name = USER
                        and instr(job_name, upper(job_name) ) > 0
                        and state = 'NOT RUNNING'
                        and attached_sessions = 0 )
        loop
            execute immediate i.stat;
        end loop;
    end;
"""
