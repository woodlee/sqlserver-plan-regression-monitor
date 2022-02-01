#!/usr/bin/env python3
import collections
import datetime
import json
import logging
import socket
import time
from datetime import datetime, timedelta
from functools import partial
from typing import List, Tuple, Set

from . import config, message_schemas, common, stats_store


logger = logging.getLogger('plan_monitor.detect')

EXCLUSION_COUNTERS = collections.defaultdict(int)


def calculate_plan_age_stats(plan_stats: stats_store.PlanStats, stats_time: int) -> Tuple[float, float]:
    plan_age_seconds = (stats_time - plan_stats.creation_time) / 1000
    last_exec_age_seconds = (stats_time - plan_stats.last_execution_time) / 1000
    return plan_age_seconds, last_exec_age_seconds


def is_established_plan(plan_age_seconds: float, last_exec_age_seconds: float) -> bool:
    return plan_age_seconds > config.MAX_NEW_PLAN_AGE_SECONDS or \
        last_exec_age_seconds > config.MAX_AGE_OF_LAST_EXECUTION_SECONDS


def is_plan_under_investigation(plan_stats: stats_store.PlanStats, stats_time: int) -> bool:
    plan_age_seconds, last_exec_age_seconds = calculate_plan_age_stats(plan_stats, stats_time)
    return not is_established_plan(plan_age_seconds, last_exec_age_seconds)


# assess which query plan hashes are recent enough to warrant investigation into badness
# so we can prevent older plans with the same query plan hash from potentially ballooning
# metrics used for determining badness
def get_query_plan_hashes_under_investigation(plans: List[stats_store.PlanStats], stats_time: int) -> Set[str]:
    return {plan_stats.worst_statement_query_plan_hash for plan_stats in plans
            if is_plan_under_investigation(plan_stats, stats_time)}


def log_exclusion(eval_msg_coordinates: str, db_identifier: str, sql_handle: str, stats_time: int,
                  trigger_msg_coordinates: str, reason: str) -> None:
    logger.debug('EXCLUDED: trigger msg %s, evaluated msg %s, db identifier %s, sql handle %s, stats_time %s, '
                 'reason: %s', trigger_msg_coordinates, eval_msg_coordinates, db_identifier, sql_handle,
                 stats_time, reason)
    EXCLUSION_COUNTERS[reason] += 1


def find_bad_plans(plans: List[stats_store.PlanStats], db_identifier: str, sql_handle: str, stats_time: int,
                   trigger_msg_coordinates: str) -> Tuple[List[stats_store.PlanStats], List[stats_store.PlanStats]]:
    prior_times, prior_reads, prior_execs, prior_plans_count, prior_last_execution = 0, 0, 0, 0, 0
    prior_plans, candidate_bad_plans, bad_plans = [], [], []
    prior_worst_plan_hashes = set()
    potential_bad_query_plan_hashes = get_query_plan_hashes_under_investigation(plans, stats_time)

    for plan_stats in plans:
        # shouldn't happen bc of the filter in the STATS_DMVS_QUERY SQL query, just being cautious:
        if plan_stats.execution_count <= 1:
            log_exclusion(plan_stats.source_stats_message_coordinates, db_identifier, sql_handle, stats_time,
                          trigger_msg_coordinates, 'Execution count low')
            continue

        plan_age_seconds, last_exec_age_seconds = calculate_plan_age_stats(plan_stats, stats_time)
        current_query_plan_hash = plan_stats.worst_statement_query_plan_hash

        if plan_age_seconds < config.MIN_NEW_PLAN_AGE_SECONDS:
            # too new; ignore entirely for now. If it's a problem we'll catch it on next poll
            log_exclusion(plan_stats.source_stats_message_coordinates, db_identifier, sql_handle, stats_time,
                          trigger_msg_coordinates, 'Too new')
            continue
        elif is_established_plan(plan_age_seconds, last_exec_age_seconds):
            # this is an old or "established" plan; gather its stats but don't consider it for "badness"
            prior_plans.append(plan_stats)
            if current_query_plan_hash not in potential_bad_query_plan_hashes:
                prior_plans_count += 1
                prior_times += plan_stats.total_elapsed_time
                prior_reads += plan_stats.total_logical_reads
                prior_execs += plan_stats.execution_count
                prior_worst_plan_hashes.add(current_query_plan_hash)
                prior_last_execution = max(prior_last_execution, plan_stats.last_execution_time)
            log_exclusion(plan_stats.source_stats_message_coordinates, db_identifier, sql_handle, stats_time,
                          trigger_msg_coordinates, 'Too old/established')
            continue
        elif plan_stats.total_elapsed_time < config.MIN_TOTAL_ELAPSED_TIME_SECONDS * 1_000_000 \
                and plan_stats.total_logical_reads < config.MIN_TOTAL_LOGICAL_READS:
            # the plan does not yet meet the execution time or reads threshold to be considered bad
            log_exclusion(plan_stats.source_stats_message_coordinates, db_identifier, sql_handle, stats_time,
                          trigger_msg_coordinates, 'Total time/reads not high enough')
            continue
        elif plan_stats.execution_count < config.MIN_EXECUTION_COUNT \
                and plan_age_seconds < config.MIN_AGE_IN_LIEU_OF_EXEC_COUNT_SECONDS:
            # the plan has not existed or executed enough it to establish its consistent badness
            log_exclusion(plan_stats.source_stats_message_coordinates, db_identifier, sql_handle, stats_time,
                          trigger_msg_coordinates, 'Total age/execs not high enough')
            continue
        elif plan_stats.total_logical_writes and plan_stats.statement_count == 1:
            # not gonna flush single statements that do writes
            log_exclusion(plan_stats.source_stats_message_coordinates, db_identifier, sql_handle, stats_time,
                          trigger_msg_coordinates, 'Writing query')
            continue
        else:
            candidate_bad_plans.append(plan_stats)

    # need enough executions prior plans to be able to trust them as a point of comparison
    if prior_plans_count and candidate_bad_plans and prior_execs > config.MIN_EXECUTION_COUNT:
        avg_prior_time = prior_times / prior_execs
        avg_prior_reads = prior_reads / prior_execs

        for plan_stats in candidate_bad_plans:
            if prior_worst_plan_hashes == {plan_stats.worst_statement_query_plan_hash}:
                # prior and candidate plans are logically equivalent, at least on their slowest query, so don't
                # bother evicting
                log_exclusion(plan_stats.source_stats_message_coordinates, db_identifier, sql_handle, stats_time,
                              trigger_msg_coordinates, 'Same plan as prior')
                continue

            avg_time = plan_stats.total_elapsed_time / plan_stats.execution_count
            avg_reads = plan_stats.total_logical_reads / plan_stats.execution_count
            time_increase_factor = avg_time / avg_prior_time
            read_increase_factor = (avg_reads / avg_prior_reads) if avg_prior_reads else 0

            if time_increase_factor > config.MIN_TIME_INCREASE_FACTOR or \
                    (time_increase_factor > 1 and read_increase_factor > config.MIN_READS_INCREASE_FACTOR):
                bad_plans.append(plan_stats)
                msg = f'''Detected bad plan:

DB identifier:                    {db_identifier}
SQL handle:                       {sql_handle}
Bad plan handle:                  {plan_stats.plan_handle}
Worst stmt offset:                {plan_stats.worst_statement_start_offset}
Worst stmt query hash:            {plan_stats.worst_statement_query_hash}
Worst stmt plan hash:             {plan_stats.worst_statement_query_plan_hash}
Plan performs writes:             {plan_stats.total_logical_writes > 0}
Statements in plan:               {plan_stats.statement_count}
Prior worst stmt plan hash(es):   {prior_worst_plan_hashes}
Last execution among prior plans: {common.format_ts(prior_last_execution)} UTC
Bad plan created:                 {common.format_ts(plan_stats.creation_time)} UTC
Bad plan last executed:           {common.format_ts(plan_stats.last_execution_time)} UTC
Stats sample time:                {common.format_ts(plan_stats.stats_query_time)} UTC
-----
 Executions since creation:  {plan_stats.execution_count:>11,} execs
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
            else:
                log_exclusion(plan_stats.source_stats_message_coordinates, db_identifier, sql_handle, stats_time,
                              trigger_msg_coordinates, 'Increase factors not satisfied')
    else:
        log_exclusion(trigger_msg_coordinates, db_identifier, sql_handle, stats_time, trigger_msg_coordinates,
                      'Current plan likely skipped due to low prior execs of other plans')

    return bad_plans, prior_plans


def detect() -> None:
    kafka_consumer = common.build_consumer(f'sqlserver_plan_regression_monitor_detect_{socket.getfqdn()}', False,
                                           message_schemas.QUERY_STATS_MESSAGE_KEY_AVRO_SCHEMA,
                                           message_schemas.QUERY_STATS_MESSAGE_VALUE_AVRO_SCHEMA)
    kafka_producer = common.build_producer(message_schemas.BAD_PLANS_MESSAGE_KEY_AVRO_SCHEMA,
                                           message_schemas.BAD_PLANS_MESSAGE_VALUE_AVRO_SCHEMA)
    kafka_consumer.subscribe([config.STATS_TOPIC],
                             on_assign=partial(common.set_offsets_to_time, config.REFRESH_INTERVAL_MINUTES * 60))
    plan_stats_store: stats_store.PlanStatsStore = stats_store.PlanStatsStore()
    successive_errors = 0

    try:
        while True:
            memory_flush_deadline: datetime.datetime = \
                datetime.utcnow() + timedelta(minutes=config.REFRESH_INTERVAL_MINUTES)

            while datetime.utcnow() < memory_flush_deadline:
                msg = kafka_consumer.poll(1.0)

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

                msg_val = dict(msg.value())
                db_identifier: str = msg_val['db_identifier']
                sql_handle: str = msg_val['sql_handle']
                stats_query_time: int = msg_val['stats_query_time']
                set_options: int = msg_val['set_options']
                msg_coordinates: str = common.msg_coordinates(msg)

                caught_up: bool = time.time() - (stats_query_time / 1000) < config.MAX_ALLOWED_EVALUATION_LAG_SECONDS

                if msg.offset() % 100_000 == 0:
                    logger.info(f'Reached {common.format_msg_info(msg)}. Caught up = {caught_up}. Queries cached: '
                                f'{len(plan_stats_store):,} Exclusions: {json.dumps(EXCLUSION_COUNTERS)}')
                if msg.offset() % 1000 == 0:
                    kafka_producer.poll(0)  # serve delivery callbacks if needed

                stats = plan_stats_store.register_stats_from_message(msg_val, msg_coordinates)

                if caught_up and len(stats) > 1:  # need other plans to compare to be able to call it "bad"
                    bad_plans, prior_plans = find_bad_plans(stats, db_identifier, sql_handle,
                                                            stats_query_time, msg_coordinates)
                    for bad_plan in bad_plans:
                        kafka_producer.poll(0)  # serve delivery callbacks
                        out_msg_val = bad_plan.to_dict()
                        out_msg_val['prior_plans'] = [p.to_dict() for p in prior_plans]
                        out_msg_val['db_identifier'] = db_identifier
                        out_msg_val['sql_handle'] = sql_handle
                        out_msg_val['set_options'] = set_options
                        out_msg_key = message_schemas.key_from_value(msg_val)
                        try:
                            kafka_producer.produce(topic=config.BAD_PLANS_TOPIC, key=out_msg_key, value=out_msg_val)
                        except Exception:
                            logger.error('Error producing message to Kafka. Logging for diagnosis:\ntopic: %s\nkey: '
                                         '%s\nvalue: %s\n', config.BAD_PLANS_TOPIC, json.dumps(out_msg_key, indent=4),
                                         json.dumps(out_msg_val, indent=4))
                            raise
                        logger.debug(f'Produced message with key {out_msg_key} and value {out_msg_val}')

            logger.info('Clearing %s queries from memory and reloading from source Kafka topic...',
                        len(plan_stats_store))
            plan_stats_store: stats_store.PlanStatsStore = stats_store.PlanStatsStore()
            common.set_offsets_to_time(config.REFRESH_INTERVAL_MINUTES * 60,
                                       kafka_consumer, kafka_consumer.assignment())
    except KeyboardInterrupt:
        logger.info('Received interrupt request; shutting down...')
        logger.info(f'Final cached queries count was: {len(plan_stats_store):,}')
    finally:
        kafka_consumer.close()
        kafka_producer.flush(10)
        logger.info('Clean shutdown complete.')


if __name__ == "__main__":
    logger.info('Starting...')
    detect()
