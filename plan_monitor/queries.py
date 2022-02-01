from datetime import datetime
from typing import NamedTuple


STATS_DMVS_QUERY = '''
WITH recent_plans AS (
    SELECT
        DISTINCT qs.plan_handle AS plan_handle
        , FIRST_VALUE(qs.statement_start_offset) OVER (PARTITION BY qs.plan_handle ORDER BY qs.total_elapsed_time DESC) 
            AS worst_statement_start_offset
        , FIRST_VALUE(qs.query_hash) OVER (PARTITION BY qs.plan_handle ORDER BY qs.total_elapsed_time DESC) 
            AS worst_statement_query_hash
        , FIRST_VALUE(qs.query_plan_hash) OVER (PARTITION BY qs.plan_handle ORDER BY qs.total_elapsed_time DESC) 
            AS worst_statement_query_plan_hash
    FROM sys.dm_exec_query_stats AS qs
    CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) AS epa
    WHERE epa.attribute = 'dbid'
        AND epa.value = DB_ID()
        AND qs.execution_count > 1
        AND qs.last_execution_time > ?
), agged AS (
    SELECT
        qs.plan_handle AS plan_handle
        , qs.sql_handle AS sql_handle
        , CAST(epa.value AS INT) AS set_options
        , MIN(qs.creation_time) AS creation_time
        , MAX(qs.last_execution_time) AS last_execution_time
        , MAX(qs.execution_count) AS execution_count
        , SUM(qs.total_worker_time) AS total_worker_time
        , SUM(qs.total_elapsed_time) AS total_elapsed_time
        , SUM(qs.total_logical_reads) AS total_logical_reads
        , SUM(qs.total_logical_writes) AS total_logical_writes
        , MAX(recent_plans.worst_statement_start_offset) AS worst_statement_start_offset
        , MAX(recent_plans.worst_statement_query_hash) AS worst_statement_query_hash
        , MAX(recent_plans.worst_statement_query_plan_hash) AS worst_statement_query_plan_hash
        , COUNT(*) AS statement_count
        , GETDATE() AS stats_query_time
    FROM recent_plans
    JOIN sys.dm_exec_query_stats AS qs ON (recent_plans.plan_handle = qs.plan_handle)
    CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) AS epa
    WHERE epa.attribute = 'set_options'
    GROUP BY qs.plan_handle, qs.sql_handle, epa.value
)
SELECT * FROM agged
WHERE (total_logical_writes = 0 OR statement_count > 1)
   AND DATEDIFF(SECOND, creation_time, GETDATE()) > ?
   AND (execution_count > ? OR DATEDIFF(SECOND, creation_time, GETDATE()) > ?)
'''


class StatsDmvsQueryResult(NamedTuple):
    plan_handle: bytes
    sql_handle: bytes
    set_options: int
    creation_time: datetime
    last_execution_time: datetime
    execution_count: int
    total_worker_time: int
    total_elapsed_time: int
    total_logical_reads: int
    total_logical_writes: int
    worst_statement_start_offset: int
    worst_statement_query_hash: bytes
    worst_statement_query_plan_hash: bytes
    statement_count: int
    stats_query_time: datetime


SNIFFED_PARAMS_QUERY = '''
WITH basedata AS (
    SELECT
        qs.statement_start_offset/2 AS stmt_start
        , qp.query_plan
        , charindex('<ParameterList>', qp.query_plan) + len('<ParameterList>') AS paramstart
        , charindex('</ParameterList>', qp.query_plan) AS paramend
    FROM sys.dm_exec_query_stats qs
    CROSS APPLY sys.dm_exec_text_query_plan(qs.plan_handle, qs.statement_start_offset,
        qs.statement_end_offset) qp
    WHERE qs.plan_handle = ?
), next_level AS (
    SELECT
        stmt_start
        , CASE
            WHEN paramend > paramstart
            THEN CAST(SUBSTRING(query_plan, paramstart, paramend - paramstart) AS xml)
        END AS params
    FROM basedata
)
SELECT n.stmt_start AS pos
    ,cr.c.value('@Column', 'nvarchar(128)') AS param_name
    ,cr.c.value('@ParameterCompiledValue', 'nvarchar(128)') AS param_sniffed_value
FROM next_level n
CROSS APPLY n.params.nodes('ColumnReference') AS cr(c)
ORDER BY n.stmt_start, param_name
'''

FINAL_STATS_QUERY = '''
SELECT MAX(last_execution_time), MAX(execution_count)
FROM sys.dm_exec_query_stats
WHERE plan_handle = ?
GROUP BY plan_handle
'''

UPDATED_PLAN_HANDLE_QUERY = '''
SELECT TOP 1 plan_handle, creation_time
FROM sys.dm_exec_query_stats
WHERE sql_handle = ?
    AND query_plan_hash = ?
    AND query_hash = ?
    AND statement_start_offset = ?
    AND creation_time >= ?
ORDER BY creation_time
'''

CONNECT_METADATA_QUERY = 'SELECT DB_NAME(), DATENAME(TZOFFSET , SYSDATETIMEOFFSET()), GETUTCDATE()'
PLAN_XML_QUERY = 'SELECT query_plan FROM sys.dm_exec_query_plan (?)'
PLAN_ATTRIBUTES_QUERY = 'SELECT attribute, CAST(value AS VARCHAR) AS value FROM sys.dm_exec_plan_attributes (?)'
SQL_TEXT_QUERY = 'SELECT text FROM sys.dm_exec_sql_text (?)'
EVICT_PLAN_QUERY = 'DBCC FREEPROCCACHE(?)'
