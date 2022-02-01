#!/usr/bin/env python3
import collections
import copy
import datetime
import json
import logging
import socket
from datetime import datetime, timezone, timedelta
from functools import partial
from typing import Dict, Any, Optional, Tuple

import confluent_kafka

from . import config, message_schemas, common, queries


logger = logging.getLogger('plan_monitor.evict')


def evict_plan(plan_info: Dict[str, Any]) -> Tuple[bool, Optional[Dict[str, Any]]]:
    conn = None
    plan_info = copy.deepcopy(plan_info)
    try:
        conn, db_tz = common.get_db_conn_with_failover(config.ODBC_CONN_STRINGS[plan_info['db_identifier']])
        plan_handle = bytes.fromhex(plan_info['plan_handle'].replace('0x', ''))
        sql_handle = bytes.fromhex(plan_info['sql_handle'].replace('0x', ''))
        worst_statement_query_hash = bytes.fromhex(plan_info['worst_statement_query_hash'].replace('0x', ''))
        worst_statement_query_plan_hash = bytes.fromhex(plan_info['worst_statement_query_plan_hash'].replace('0x', ''))
        statement_start_offset = plan_info['worst_statement_start_offset']
        plan_creation_time = datetime.fromtimestamp(plan_info['creation_time'] / 1000).astimezone(db_tz)

        with conn.cursor() as cursor:
            cursor.execute(queries.UPDATED_PLAN_HANDLE_QUERY, sql_handle, worst_statement_query_plan_hash,
                           worst_statement_query_hash, statement_start_offset,
                           plan_creation_time.replace(tzinfo=None) - timedelta(milliseconds=10))
            res = cursor.fetchone()
            if not res:
                logger.debug('Eviction skip reason: no result from updated handle query. Params were: %s %s %s %s %s',
                             plan_info['sql_handle'], plan_info['worst_statement_query_plan_hash'],
                             plan_info['worst_statement_query_hash'], statement_start_offset, plan_creation_time)
                return False, {}
            updated_plan_handle, updated_creation_time = res
            if updated_plan_handle != plan_handle:
                logger.info('Substituting new plan handle %s (created %s) for original handle %s (created %s)',
                            f'0x{updated_plan_handle.hex()}', updated_creation_time, plan_info['plan_handle'],
                            plan_creation_time)
                plan_handle = updated_plan_handle
                plan_info['plan_handle'] = f'0x{updated_plan_handle.hex()}'
                plan_info['creation_time'] = int(updated_creation_time.replace(tzinfo=db_tz).timestamp() * 1000)

            cursor.execute(queries.SNIFFED_PARAMS_QUERY, plan_handle)
            sniffed_params = cursor.fetchall()
            cursor.execute(queries.PLAN_XML_QUERY, plan_handle)
            plan_xml = cursor.fetchone()
            if (not plan_xml) or (not plan_xml[0]):
                logger.debug('Eviction skip reason: no plan XML')
                return False, {}
            cursor.execute(queries.PLAN_ATTRIBUTES_QUERY, plan_handle)
            plan_attributes = cursor.fetchall()
            if not plan_attributes:
                logger.debug('Eviction skip reason: no plan attributes')
                return False, {}
            cursor.execute(queries.SQL_TEXT_QUERY, sql_handle)
            sql_text = cursor.fetchone()
            if not sql_text:
                logger.debug('Eviction skip reason: no SQL text')
                return False, {}
            cursor.execute(queries.FINAL_STATS_QUERY, plan_handle)
            final_stats = cursor.fetchone()
            if not final_stats:
                logger.debug('Eviction skip reason: no final stats')
                return False, {}
            cursor.execute(queries.EVICT_PLAN_QUERY, plan_handle)
    finally:
        if conn:
            conn.close()

    plan_info["plan_xml"] = plan_xml[0]
    plan_info["plan_sniffed_parameters"] = {row[1]: row[2] for row in sniffed_params}
    plan_info["plan_attributes"] = {row[0]: row[1] or '' for row in plan_attributes}
    plan_info["sql_text"] = sql_text[0]
    plan_info["final_execution_time"] = int(final_stats[0].replace(tzinfo=db_tz).timestamp() * 1000)
    plan_info["final_execution_count"] = final_stats[1]
    plan_info["eviction_time"] = int(datetime.now(timezone.utc).timestamp() * 1000)

    return True, plan_info


def kafka_producer_delivery_cb(err: confluent_kafka.KafkaError, msg: confluent_kafka.Message) -> None:
    if err is not None:
        return common.kafka_producer_delivery_cb(err, msg)
    else:
        logger.info(f"Details of most recent eviction published to Kafka at: {common.msg_coordinates(msg)}")


def evict() -> None:
    kafka_consumer = common.build_consumer(f'sqlserver_plan_regression_monitor_evict_{socket.getfqdn()}', True,
                                           message_schemas.BAD_PLANS_MESSAGE_KEY_AVRO_SCHEMA,
                                           message_schemas.BAD_PLANS_MESSAGE_VALUE_AVRO_SCHEMA)
    kafka_producer = common.build_producer(message_schemas.EVICTED_PLANS_MESSAGE_KEY_AVRO_SCHEMA,
                                           message_schemas.EVICTED_PLANS_MESSAGE_VALUE_AVRO_SCHEMA)
    kafka_consumer.subscribe([config.BAD_PLANS_TOPIC],
                             on_assign=partial(common.set_offsets_to_time, config.MAX_ALLOWED_EVALUATION_LAG_SECONDS))
    last_log_heartbeat = datetime.utcnow()
    log_heartbeat_interval_seconds = 60
    successive_errors = 0

    try:
        throttles = collections.defaultdict(partial(collections.deque,
                                                    maxlen=config.EVICTION_THROTTLE_MAX_PLANS_FOR_TIME_WINDOW))
        while True:
            if (datetime.utcnow() - last_log_heartbeat).total_seconds() > log_heartbeat_interval_seconds:
                positions = kafka_consumer.position(kafka_consumer.assignment())
                logger.info(f'Current Kafka consumer position(s): {positions}')
                last_log_heartbeat = datetime.utcnow()

            msg = kafka_consumer.poll(1.0)
            kafka_producer.poll(0)  # serve delivery callbacks if needed

            if msg is None:
                continue

            if msg.error():
                successive_errors += 1
                logger.error("AvroConsumer error (%s of %s until exception): %s", successive_errors,
                             common.MAX_SUCCESSIVE_CONSUMER_ERRORS, msg.error())
                if successive_errors >= common.MAX_SUCCESSIVE_CONSUMER_ERRORS:
                    raise Exception("Too many AvroConsumer exceptions: {}".format(msg.error()))
                continue
            successive_errors = 0

            message_time = datetime.fromtimestamp(msg.timestamp()[1] / 1000, timezone.utc)
            message_age = (datetime.now(timezone.utc) - message_time).total_seconds()
            if message_age > config.MAX_ALLOWED_EVALUATION_LAG_SECONDS:
                logger.warning(f'Skipping message older than configured '
                               f'MAX_ALLOWED_EVALUATION_LAG_SECONDS. {common.format_msg_info(msg)}')
                last_log_heartbeat = datetime.utcnow()
                kafka_consumer.store_offsets(msg)
                continue

            db_id = msg.value()['db_identifier']
            if db_id in throttles and len(throttles[db_id]) == throttles[db_id].maxlen:
                oldest_eviction_age = (datetime.utcnow() - throttles[db_id][0]).total_seconds()
                if oldest_eviction_age < config.EVICTION_THROTTLE_TIME_WINDOW_SECONDS:
                    logger.warning(f'Skipping message due to eviction throttling. For DB identifier {db_id}, '
                                   f'{len(throttles[db_id])} plans have already been evicted in the past '
                                   f'{oldest_eviction_age} seconds (oldest: {throttles[db_id][0]}; '
                                   f'newest: {throttles[db_id][-1]}). {common.format_msg_info(msg)}')
                    last_log_heartbeat = datetime.utcnow()
                    kafka_consumer.store_offsets(msg)
                    continue

            plan_hash = msg.value()['worst_statement_query_plan_hash'].lower()
            if plan_hash in config.QUERY_PLAN_HASHES_NOT_TO_EVICT:
                logger.info(f'Skipping message with query plan hash on the not-to-evict list: {plan_hash}, in '
                            f'message {common.format_msg_info(msg)}')
                last_log_heartbeat = datetime.utcnow()
                kafka_consumer.store_offsets(msg)
                continue

            evicted, eviction_msg = evict_plan(dict(msg.value()))

            if evicted:
                msg_key = message_schemas.key_from_value(eviction_msg)
                eviction_msg["source_bad_plan_message_coordinates"] = common.msg_coordinates(msg)
                try:
                    kafka_producer.produce(topic=config.EVICTED_PLANS_TOPIC, key=msg_key, value=eviction_msg)
                except Exception:
                    logger.error('Error producing message to Kafka. Logging for diagnosis:\ntopic: %s\nkey: '
                                 '%s\nvalue: %s\n', config.EVICTED_PLANS_TOPIC, json.dumps(msg_key, indent=4),
                                 json.dumps(eviction_msg, indent=4))
                    raise
                logger.info(f'Plan evicted from DB "{db_id}" based on bad plan message at '
                            f'{common.msg_coordinates(msg)}')
                throttles[db_id].append(datetime.utcnow())
            else:
                logger.warning(f'Skipped eviction on DB "{db_id}" - the plan may have already been evicted upon '
                               f'checking the target DB. {common.format_msg_info(msg)}')
            last_log_heartbeat = datetime.utcnow()
            kafka_consumer.store_offsets(msg)
    except KeyboardInterrupt:
        logger.info('Received interrupt request; shutting down...')
    finally:
        kafka_consumer.close()
        kafka_producer.flush(10)
        logger.info('Clean shutdown complete.')


if __name__ == "__main__":
    logger.info('Starting...')
    evict()
