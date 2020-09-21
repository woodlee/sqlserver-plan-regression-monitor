#!/usr/bin/env python3
import collections
import datetime
import logging
import socket
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any

import confluent_kafka
import confluent_kafka.schema_registry.avro

from . import config, message_schemas, common


logger = logging.getLogger('plan_monitor.detect')


def find_bad_plans(plans: Dict[str, Dict], message_time: datetime) -> List[Dict[str, Any]]:
    must_created_before = message_time - timedelta(seconds=config.MIN_NEW_PLAN_AGE_SECONDS)
    must_created_after = message_time - timedelta(seconds=config.MAX_NEW_PLAN_AGE_SECONDS)
    must_executed_after = message_time - timedelta(seconds=config.MAX_AGE_OF_LAST_EXECUTION_SECONDS)
    prior_times, prior_reads, prior_execs, prior_plans_count, prior_last_execution = 0, 0, 0, 0, 0
    candidates, bad_plans = [], []
    prior_worst_plan_hashes = set()

    for plan_handle, plan_stats in plans.items():
        if plan_stats['execution_count'] <= 1:  # shouldn't happen, just being cautious
            continue

        plan_creation_time = datetime.fromtimestamp(plan_stats['creation_time'] / 1000.0, timezone.utc)
        last_execution_time = datetime.fromtimestamp(plan_stats['last_execution_time'] / 1000.0, timezone.utc)
        plan_age_seconds = (message_time - plan_creation_time).total_seconds()

        if plan_creation_time > must_created_before:
            # too new; ignore entirely for now. If it's a problem we'll catch it on next poll
            continue
        elif plan_creation_time < must_created_after or last_execution_time < must_executed_after:
            # this is an old or "established" plan; gather its stats but don't consider it for "badness"
            prior_plans_count += 1
            prior_times += plan_stats['total_elapsed_time']
            prior_reads += plan_stats['total_logical_reads']
            prior_execs += plan_stats['execution_count']
            prior_worst_plan_hashes.add(plan_stats['worst_statement_query_plan_hash'])
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
            candidates.append(plan_stats)

    # need enough executions prior plans to be able to trust them as a point of comparison
    if prior_plans_count and candidates and prior_execs > config.MIN_EXECUTION_COUNT:
        avg_prior_time = prior_times / prior_execs
        avg_prior_reads = prior_reads / prior_execs

        for plan_stats in candidates:
            if prior_worst_plan_hashes == {plan_stats['worst_statement_query_plan_hash']}:
                # prior and candidate plans are logically equivalent, at least on their slowest query, so don't
                # bother evicting
                continue

            avg_time = plan_stats['total_elapsed_time'] / plan_stats['execution_count']
            avg_reads = plan_stats['total_logical_reads'] / plan_stats['execution_count']
            time_increase_factor = avg_time / avg_prior_time
            read_increase_factor = avg_reads / avg_prior_reads

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
           Increase factor: {(avg_time / avg_prior_time):>6.1f}x
-----
Avg logical reads per exec:   {avg_reads:>10,.0f} reads
      vs. {prior_plans_count:>2} prior plan(s):   {avg_prior_reads:>10,.0f} reads
           Increase factor: {((avg_reads / avg_prior_reads) if avg_prior_reads else 0):>6.1f}x
'''
                logger.info(msg)

    return bad_plans


def set_offsets(consumer: confluent_kafka.DeserializingConsumer,
                partitions: List[confluent_kafka.TopicPartition]) -> None:
    start_from = datetime.now(timezone.utc) - timedelta(minutes=config.REFRESH_INTERVAL_MINUTES)
    logger.info('Setting consumer offsets to start from %s', start_from)
    for p in partitions:
        p.offset = int(start_from.timestamp() * 1000)  # yep, it's a weird API
    consumer.assign(partitions)
    for p in consumer.offsets_for_times(partitions):
        logger.debug('Topic %s partition %s SEEKing to offset %s', p.topic, p.partition, p.offset)
        consumer.seek(p)


def detect() -> None:
    schema_registry = confluent_kafka.schema_registry.SchemaRegistryClient({'url': config.SCHEMA_REGISTRY_URL})
    key_serializer = confluent_kafka.schema_registry.avro.AvroSerializer(
        message_schemas.STATS_MESSAGE_KEY_AVRO_SCHEMA, schema_registry)
    value_serializer = confluent_kafka.schema_registry.avro.AvroSerializer(
        message_schemas.STATS_MESSAGE_VALUE_AVRO_SCHEMA, schema_registry)
    key_deserializer = confluent_kafka.schema_registry.avro.AvroDeserializer(
        message_schemas.STATS_MESSAGE_KEY_AVRO_SCHEMA, schema_registry)
    value_deserializer = confluent_kafka.schema_registry.avro.AvroDeserializer(
        message_schemas.STATS_MESSAGE_VALUE_AVRO_SCHEMA, schema_registry)

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
    kafka_consumer.subscribe([config.STATS_TOPIC], on_assign=set_offsets)

    try:
        while True:
            queries = collections.defaultdict(dict)
            memory_flush_deadline = datetime.utcnow() + timedelta(minutes=config.REFRESH_INTERVAL_MINUTES)

            while datetime.utcnow() < memory_flush_deadline:
                msg = kafka_consumer.poll(1.0)

                if msg is None:
                    continue

                message_time = datetime.fromtimestamp(msg.timestamp()[1] / 1000, timezone.utc)
                caught_up = (datetime.now(timezone.utc) - message_time).total_seconds() < \
                    config.MAX_ALLOWED_EVALUATION_LAG_SECONDS
                key_tup = (msg.key()['db_identifier'], msg.key()['set_options'], msg.key()['sql_handle'])
                queries[key_tup][msg.value()['plan_handle']] = dict(msg.value())

                if msg.offset() % 100_000 == 0:
                    logger.info(f'Reached partition {msg.partition()}, offset {msg.offset():,}, timestamp '
                                f'{common.format_ts(msg.timestamp()[1])}. Caught up = {caught_up}. Queries cached: '
                                f'{len(queries):,}')
                if msg.offset() % 1000 == 0:
                    kafka_producer.poll(0)  # serve delivery callbacks if needed

                if caught_up and len(queries[key_tup]) > 1:  # need other plans to compare to be able to call it "bad"
                    bad_plans = find_bad_plans(queries[key_tup], message_time)
                    for bad_plan in bad_plans:
                        kafka_producer.poll(0)  # serve delivery callbacks
                        msg_key = {"db_identifier": bad_plan["db_identifier"],
                                   "set_options": bad_plan["set_options"],
                                   "sql_handle": bad_plan["sql_handle"]}
                        kafka_producer.produce(topic=config.BAD_PLANS_TOPIC, key=msg_key, value=bad_plan,
                                               on_delivery=common.kafka_producer_delivery_cb)

            logger.info('Clearing %s queries from memory and reloading from source Kafka topic...', len(queries))
            set_offsets(kafka_consumer, kafka_consumer.assignment())
    except KeyboardInterrupt:
        logger.info('Received interrupt request; shutting down...')
    finally:
        kafka_consumer.close()
        kafka_producer.flush(10)
        logger.info('Clean shutdown complete.')


if __name__ == "__main__":
    logger.info('Starting...')
    detect()
