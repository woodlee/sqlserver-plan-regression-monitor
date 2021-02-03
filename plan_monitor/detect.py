#!/usr/bin/env python3
import collections
import datetime
import logging
import socket
import time
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Dict, List, Any, Tuple

import confluent_kafka
import confluent_kafka.schema_registry.avro

from . import config, message_schemas, common


logger = logging.getLogger('plan_monitor.detect')

def calculate_plan_age_stats(plan_stats: Dict, stats_time: int) -> Tuple(int, int):
    plan_age_seconds = (stats_time - plan_stats['creation_time']) / 1000
    last_exec_age_seconds = (stats_time - plan_stats['last_execution_time']) / 1000
    return (plan_age_seconds, last_exec_age_seconds)

def is_established_plan(plan_age_seconds: int, last_exec_age_seconds: int) -> bool:
    return plan_age_seconds > config.MAX_NEW_PLAN_AGE_SECONDS or \
            last_exec_age_seconds > config.MAX_AGE_OF_LAST_EXECUTION_SECONDS

def is_plan_under_investigation(plan_stats: Dict, stats_time: int) -> bool:
    plan_age_seconds, last_exec_age_seconds = calculate_plan_age_stats(plan_stats, stats_time)
    return not is_established_plan(plan_age_seconds, last_exec_age_seconds)

def find_bad_plans(plans: Dict[str, Dict], stats_time: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    prior_times, prior_reads, prior_execs, prior_plans_count, prior_last_execution = 0, 0, 0, 0, 0
    prior_plans, candidate_bad_plans, bad_plans = [], [], []
    prior_worst_plan_hashes = set()
    potential_bad_plan_hashes = {plan_stats['worst_statement_query_plan_hash'] for plan_handle, plan_stats in plans.items() \
        if is_plan_under_investigation(plan_stats, stats_time)}

    for plan_handle, plan_stats in plans.items():
        # shouldn't happen bc of the filter in the STATS_DMVS_QUERY SQL query, just being cautious:
        if plan_stats['execution_count'] <= 1:
            continue
        
        plan_age_seconds, last_exec_age_seconds = calculate_plan_age_stats(plan_stats, stats_time)
        current_query_plan_hash = plan_stats['worst_statement_query_plan_hash']

        if plan_age_seconds < config.MIN_NEW_PLAN_AGE_SECONDS:
            # too new; ignore entirely for now. If it's a problem we'll catch it on next poll
            continue
        elif is_established_plan(plan_age_seconds, last_exec_age_seconds) and \
                current_query_plan_hash not in potential_bad_plan_hashes:
            # this is an old or "established" plan; gather its stats but don't consider it for "badness"
            prior_plans.append(plan_stats)
            prior_plans_count += 1
            prior_times += plan_stats['total_elapsed_time']
            prior_reads += plan_stats['total_logical_reads']
            prior_execs += plan_stats['execution_count']
            prior_worst_plan_hashes.add(current_query_plan_hash)
            prior_last_execution = max(prior_last_execution, plan_stats['last_execution_time'])
            continue
        elif plan_stats['total_elapsed_time'] < config.MIN_TOTAL_ELAPSED_TIME_SECONDS * 1_000_000 \
                and plan_stats['total_logical_reads'] < config.MIN_TOTAL_LOGICAL_READS:
            # the plan does not yet meet the execution time or reads threshold to be considered bad
            continue
        elif plan_stats['execution_count'] < config.MIN_EXECUTION_COUNT \
                and plan_age_seconds < config.MIN_AGE_IN_LIEU_OF_EXEC_COUNT_SECONDS:
            # the plan has not existed or executed enough it to establish its consistent badness
            continue
        elif plan_stats['total_logical_writes'] and plan_stats['statement_count'] == 1:
            # not gonna flush single statements that do writes
            continue
        else:
            candidate_bad_plans.append(plan_stats)

    # need enough executions prior plans to be able to trust them as a point of comparison
    if prior_plans_count and candidate_bad_plans and prior_execs > config.MIN_EXECUTION_COUNT:
        avg_prior_time = prior_times / prior_execs
        avg_prior_reads = prior_reads / prior_execs

        for plan_stats in candidate_bad_plans:
            if prior_worst_plan_hashes == {plan_stats['worst_statement_query_plan_hash']}:
                # prior and candidate plans are logically equivalent, at least on their slowest query, so don't
                # bother evicting
                continue

            avg_time = plan_stats['total_elapsed_time'] / plan_stats['execution_count']
            avg_reads = plan_stats['total_logical_reads'] / plan_stats['execution_count']
            time_increase_factor = avg_time / avg_prior_time
            read_increase_factor = (avg_reads / avg_prior_reads) if avg_prior_reads else 0

            if time_increase_factor > config.MIN_TIME_INCREASE_FACTOR or \
                    (time_increase_factor > 1 and read_increase_factor > config.MIN_READS_INCREASE_FACTOR):
                bad_plans.append(plan_stats)
                msg = f'''Detected bad plan:

DB identifier:                    {plan_stats['db_identifier']}
SQL handle:                       {plan_stats['sql_handle']}
Bad plan handle:                  {plan_stats['plan_handle']}
Worst stmt offset:                {plan_stats['worst_statement_start_offset']}
Worst stmt query hash:            {plan_stats['worst_statement_query_hash']}
Worst stmt plan hash:             {plan_stats['worst_statement_query_plan_hash']}
Plan performs writes:             {plan_stats['total_logical_writes'] > 0}
Statements in plan:               {plan_stats['statement_count']}
Prior worst stmt plan hash(es):   {prior_worst_plan_hashes}
Last execution among prior plans: {common.format_ts(prior_last_execution)} UTC
Bad plan created:                 {common.format_ts(plan_stats['creation_time'])} UTC
Bad plan last executed:           {common.format_ts(plan_stats['last_execution_time'])} UTC
Stats sample time:                {common.format_ts(plan_stats['stats_query_time'])} UTC
-----
 Executions since creation:  {plan_stats['execution_count']:>11,} execs
      vs. {prior_plans_count:>2} prior plan(s):  {prior_execs:>11,} execs
-----
 Avg elapsed time per exec:      {(avg_time / 1000):>7.1f} ms
      vs. {prior_plans_count:>2} prior plan(s):      {(avg_prior_time / 1000):>7.1f} ms
           Increase factor: {time_increase_factor:>6.1f}x
-----
Avg logical reads per exec:   {avg_reads:>10,.0f} reads
      vs. {prior_plans_count:>2} prior plan(s):   {avg_prior_reads:>10,.0f} reads
           Increase factor: {read_increase_factor:>6.1f}x
'''
                logger.info(msg)

    return bad_plans, prior_plans

def detect() -> None:
    schema_registry = confluent_kafka.schema_registry.SchemaRegistryClient({'url': config.SCHEMA_REGISTRY_URL})
    key_serializer = confluent_kafka.schema_registry.avro.AvroSerializer(
        message_schemas.BAD_PLANS_MESSAGE_KEY_AVRO_SCHEMA, schema_registry)
    value_serializer = confluent_kafka.schema_registry.avro.AvroSerializer(
        message_schemas.BAD_PLANS_MESSAGE_VALUE_AVRO_SCHEMA, schema_registry)
    key_deserializer = confluent_kafka.schema_registry.avro.AvroDeserializer(
        message_schemas.QUERY_STATS_MESSAGE_KEY_AVRO_SCHEMA, schema_registry)
    value_deserializer = confluent_kafka.schema_registry.avro.AvroDeserializer(
        message_schemas.QUERY_STATS_MESSAGE_VALUE_AVRO_SCHEMA, schema_registry)

    producer_config = {'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
                       'key.serializer': key_serializer,
                       'value.serializer': value_serializer,
                       'linger.ms': 100,
                       'retry.backoff.ms': 250,
                       'compression.codec': 'snappy'}
    consumer_config = {'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
                       'group.id': f'sqlserver_plan_regression_monitor_detect_{socket.getfqdn()}',
                       'key.deserializer': key_deserializer,
                       'value.deserializer': value_deserializer,
                       # We manage our own offset seeking and do not use commits in this module:
                       'enable.auto.commit': False,
                       'error_cb': lambda evt: logger.error('Kafka error: %s', evt),
                       'throttle_cb': lambda evt: logger.warning('Kafka throttle event: %s', evt)}

    kafka_producer = confluent_kafka.SerializingProducer(producer_config)
    kafka_consumer = confluent_kafka.DeserializingConsumer(consumer_config)
    kafka_consumer.subscribe(
        [config.STATS_TOPIC], on_assign=partial(common.set_offsets_to_time, config.REFRESH_INTERVAL_MINUTES * 60))

    try:
        while True:
            # Top level: one per query on a particular DB. Next level: plans seen for that SQL query, keyed by the
            # plan_handle. 3rd and innermost level: the dict of stats collected for the plan
            queries = collections.defaultdict(dict)

            memory_flush_deadline = datetime.utcnow() + timedelta(minutes=config.REFRESH_INTERVAL_MINUTES)

            while datetime.utcnow() < memory_flush_deadline:
                msg = kafka_consumer.poll(1.0)

                if msg is None:
                    continue

                msg_key = dict(msg.key())
                msg_val = dict(msg.value())

                caught_up = time.time() - (msg_val['stats_query_time'] / 1000) < \
                    config.MAX_ALLOWED_EVALUATION_LAG_SECONDS
                query_key = (msg_key['db_identifier'], msg_key['set_options'], msg_key['sql_handle'])
                queries[query_key][msg_val['plan_handle']] = msg_val
                queries[query_key][msg_val['plan_handle']]['source_stats_message_coordinates'] = \
                    common.msg_coordinates(msg)

                if msg.offset() % 100_000 == 0:
                    logger.info(f'Reached {common.format_msg_info(msg)}. Caught up = {caught_up}. Queries cached: '
                                f'{len(queries):,}')
                if msg.offset() % 1000 == 0:
                    kafka_producer.poll(0)  # serve delivery callbacks if needed

                if caught_up and len(queries[query_key]) > 1:  # need other plans to compare to be able to call it "bad"
                    bad_plans, prior_plans = find_bad_plans(queries[query_key], msg_val['stats_query_time'])
                    for bad_plan in bad_plans:
                        kafka_producer.poll(0)  # serve delivery callbacks
                        bad_plan['prior_plans'] = prior_plans
                        msg_key = message_schemas.key_from_value(bad_plan)
                        kafka_producer.produce(topic=config.BAD_PLANS_TOPIC, key=msg_key, value=bad_plan,
                                               on_delivery=common.kafka_producer_delivery_cb)
                        logger.debug(f'Produced message with key {msg_key} and value {bad_plan}')

            logger.info('Clearing %s queries from memory and reloading from source Kafka topic...', len(queries))
            common.set_offsets_to_time(config.REFRESH_INTERVAL_MINUTES * 60,
                                       kafka_consumer, kafka_consumer.assignment())
    except KeyboardInterrupt:
        logger.info('Received interrupt request; shutting down...')
    finally:
        kafka_consumer.close()
        kafka_producer.flush(10)
        logger.info('Clean shutdown complete.')


if __name__ == "__main__":
    logger.info('Starting...')
    detect()
