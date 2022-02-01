import json
from typing import Any, Dict

from . import config


# The SQL query that pulls the query stats also includes the plan_handle as part of its grouping key. However, for the
# Kafka messages we want to ensure that all plans for the same sql_handle are in the same Kafka partition so that they
# are handled by the same detect-step consumer, so the plan_handle is not included here:
def get_key_schema(record_name: str) -> str:
    return json.dumps({
        "name": record_name,
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


def key_from_value(message_value: Dict[str, Any]) -> Dict[str, Any]:
    return {"db_identifier": message_value["db_identifier"],
            "set_options": message_value["set_options"],
            "sql_handle": message_value["sql_handle"]}


QUERY_STATS_MESSAGE_KEY_AVRO_SCHEMA = get_key_schema('query_stats_key')
BAD_PLANS_MESSAGE_KEY_AVRO_SCHEMA = get_key_schema('bad_plans_key')
EVICTED_PLANS_MESSAGE_KEY_AVRO_SCHEMA = get_key_schema('evicted_plans_key')

SINGLE_PLAN_STATS_FIELDS = [
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

QUERY_STATS_MESSAGE_VALUE_AVRO_SCHEMA = json.dumps({
    "name": "query_stats_value",
    "namespace": config.AVRO_SCHEMA_NAMESPACE,
    "type": "record",
    "fields": SINGLE_PLAN_STATS_FIELDS
})

PRIOR_PLANS_SCHEMA = [x for x in SINGLE_PLAN_STATS_FIELDS
                      if x['name'] not in ('db_identifier', 'sql_handle', 'set_options')]

BAD_PLANS_MESSAGE_VALUE_AVRO_SCHEMA = json.dumps({
    "name": "bad_plans_value",
    "namespace": config.AVRO_SCHEMA_NAMESPACE,
    "type": "record",
    "fields": SINGLE_PLAN_STATS_FIELDS + [
        {
            "name": "prior_plans",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "prior_plan",
                    "fields": PRIOR_PLANS_SCHEMA
                }
            }
        },
        {
            "name": "source_stats_message_coordinates",
            "type": "string"
        }
    ]
})

EVICTED_PLANS_MESSAGE_VALUE_AVRO_SCHEMA = json.dumps({
    "name": "evicted_plans_value",
    "namespace": config.AVRO_SCHEMA_NAMESPACE,
    "type": "record",
    "fields": SINGLE_PLAN_STATS_FIELDS + [
        {
            "name": "prior_plans",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "prior_plan",
                    "fields": PRIOR_PLANS_SCHEMA
                }
            }
        },
        {
            "name": "source_stats_message_coordinates",
            "type": "string"
        },
        {
            "name": "source_bad_plan_message_coordinates",
            "type": "string"
        },
        {
            "name": "plan_xml",
            "type": "string"
        },
        {
            "name": "plan_sniffed_parameters",
            "type": {
                "type": "map",
                "values": "string"
            }
        },
        {
            "name": "plan_attributes",
            "type": {
                "type": "map",
                "values": "string"
            }
        },
        {
            "name": "sql_text",
            "type": "string"
        },
        {
            "name": "final_execution_time",
            "type": "long",
            "logicalType": "timestamp-millis"
        },
        {
            "name": "final_execution_count",
            "type": "long"
        },
        {
            "name": "eviction_time",
            "type": "long",
            "logicalType": "timestamp-millis"
        }
    ]
})
