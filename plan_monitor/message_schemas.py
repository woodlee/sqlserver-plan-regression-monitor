import json

from . import config


STATS_MESSAGE_KEY_AVRO_SCHEMA = json.dumps({
    "name": "query_stats_key",
    "namespace": config.AVRO_SCHEMA_NAMESPACE,
    "type": "record",
    "fields": [
        {
            "name": "db_identifier",
            "type": "string"
        },
        {
            "name": "set_options",
            "type": "int"
        },
        {
            "name": "sql_handle",
            "type": "string"
        }
    ]
})


STATS_MESSAGE_VALUE_AVRO_SCHEMA = json.dumps({
    "name": "query_stats_value",
    "namespace": config.AVRO_SCHEMA_NAMESPACE,
    "type": "record",
    "fields": [
        {
            "name": "db_identifier",
            "type": "string"
        },
        {
            "name": "plan_handle",
            "type": "string"
        },
        {
            "name": "sql_handle",
            "type": "string"
        },
        {
            "name": "set_options",
            "type": "int"
        },
        {
            "name": "creation_time",
            "type": "long",
            "logicalType": "timestamp-millis"
        },
        {
            "name": "last_execution_time",
            "type": "long",
            "logicalType": "timestamp-millis"
        },
        {
            "name": "execution_count",
            "type": "long"
        },
        {
            "name": "total_worker_time",
            "type": "long"
        },
        {
            "name": "total_elapsed_time",
            "type": "long"
        },
        {
            "name": "total_logical_reads",
            "type": "long"
        },
        {
            "name": "total_logical_writes",
            "type": "long"
        },
        {
            "name": "worst_statement_start_offset",
            "type": "int"
        },
        {
            "name": "worst_statement_query_hash",
            "type": "string"
        },
        {
            "name": "worst_statement_query_plan_hash",
            "type": "string"
        },
        {
            "name": "statement_count",
            "type": "int"
        },
        {
            "name": "stats_query_time",
            "type": "long",
            "logicalType": "timestamp-millis"
        }
    ]
})
