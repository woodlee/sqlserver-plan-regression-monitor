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
)
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
