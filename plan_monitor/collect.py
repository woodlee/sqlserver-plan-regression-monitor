#!/usr/bin/env python3
import datetime
import logging
import multiprocessing as mp
import queue
import time
from datetime import datetime, timedelta
from typing import Dict, Any

from . import config, queries, message_schemas, common


logger = logging.getLogger('plan_monitor.collect')


def poll_db(db_identifier: str, odbc_conn_string: str, stop_event: mp.Event,
            result_queue: 'mp.Queue[Dict[str, Any]]') -> None:
    exit_message_logged = False
    conn = None

    try:
        conn, db_tz = common.get_db_conn_with_failover(odbc_conn_string)
        next_poll_due = datetime.utcnow()
        read_executions_from = (datetime.now(db_tz) -
                                timedelta(minutes=config.REFRESH_INTERVAL_MINUTES)).replace(tzinfo=None)
        while not stop_event.is_set():
            if datetime.utcnow() < next_poll_due:
                time.sleep(0.1)
                continue

            next_poll_due = datetime.utcnow() + timedelta(seconds=config.DB_STATS_POLL_INTERVAL_SECONDS)

            with conn.cursor() as cursor:
                query_executions_since = read_executions_from - timedelta(seconds=1)  # "slop factor"
                q_start = time.perf_counter()
                cursor.execute(queries.STATS_DMVS_QUERY, query_executions_since, config.MIN_NEW_PLAN_AGE_SECONDS,
                               config.MIN_EXECUTION_COUNT, config.MIN_AGE_IN_LIEU_OF_EXEC_COUNT_SECONDS)
                count = 0
                while not stop_event.is_set():
                    rows = cursor.fetchmany(config.STATS_ROW_FETCH_SIZE)
                    if not rows:
                        break
                    for row in rows:
                        count += 1
                        row = queries.StatsDmvsQueryResult(*row)
                        if row.last_execution_time > read_executions_from:
                            read_executions_from = row.last_execution_time
                        result_queue.put({
                            "db_identifier": db_identifier,
                            "plan_handle": f'0x{row.plan_handle.hex()}',
                            "sql_handle": f'0x{row.sql_handle.hex()}',
                            "set_options": row.set_options,
                            "creation_time": int(row.creation_time.replace(tzinfo=db_tz).timestamp() * 1000),
                            "last_execution_time": int(
                                row.last_execution_time.replace(tzinfo=db_tz).timestamp() * 1000),
                            "execution_count": row.execution_count,
                            "total_worker_time": row.total_worker_time,
                            "total_elapsed_time": row.total_elapsed_time,
                            "total_logical_reads": row.total_logical_reads,
                            "total_logical_writes": row.total_logical_writes,
                            "worst_statement_start_offset": row.worst_statement_start_offset,
                            "worst_statement_query_hash": f'0x{row.worst_statement_query_hash.hex()}',
                            "worst_statement_query_plan_hash": f'0x{row.worst_statement_query_plan_hash.hex()}',
                            "statement_count": row.statement_count,
                            "stats_query_time": int(row.stats_query_time.replace(tzinfo=db_tz).timestamp() * 1000),
                        }, timeout=5.0)

                q_time = int((time.perf_counter() - q_start) * 1000)
                logger.debug(f'Poll retrieved {count:,} rows in {q_time:,} ms.')
    except KeyboardInterrupt:
        logger.info('Exiting due to external interrupt request.')
        exit_message_logged = True
    except Exception:
        logger.info('Exiting due to unhandled exception.')
        exit_message_logged = True
        raise
    finally:
        stop_event.set()
        result_queue.close()
        if conn:
            conn.close()
        if not exit_message_logged:
            logger.info('Exiting due to shutdown initiated by another process.')


def collect() -> None:
    kafka_producer = common.build_producer(message_schemas.QUERY_STATS_MESSAGE_KEY_AVRO_SCHEMA,
                                           message_schemas.QUERY_STATS_MESSAGE_VALUE_AVRO_SCHEMA)
    result_queue = mp.Queue(10000)
    stop_event = mp.Event()
    produced_count = 0
    processes = []

    for db_identifier, odbc_conn_string in config.ODBC_CONN_STRINGS.items():
        processes.append(mp.Process(target=poll_db, name=f'db-{db_identifier}',
                                    args=(db_identifier, odbc_conn_string, stop_event, result_queue)))

    start_time = time.perf_counter()

    try:
        started_ct = 0
        for process in processes:
            if stop_event.is_set():
                break
            process.start()
            started_ct += 1
            # Stagger so the herd doesn't dump a ton onto the results queue or Kafka producer at once:
            time.sleep(0.5)

        logger.info(f'Started {started_ct} DB poll subprocesses')

        while not (stop_event.is_set() and result_queue.empty()):
            kafka_producer.poll(0)  # serve delivery callbacks
            try:
                msg_value = result_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            msg_key = message_schemas.key_from_value(msg_value)
            kafka_producer.produce(topic=config.STATS_TOPIC, key=msg_key, value=msg_value)
            produced_count += 1
            if produced_count % 100_000 == 0:
                logger.info(f"Produced {produced_count:,} stats records since process start...")
    except KeyboardInterrupt:
        logger.info('Received interrupt request; shutting down...')
    finally:
        stop_event.set()
        elapsed = (time.perf_counter() - start_time)
        logger.info(f'Exiting after {produced_count:,} records were produced in {elapsed:.1f} seconds. Cleaning up...')
        for process in processes:
            if process.is_alive():
                process.join(timeout=0.5)
            if process.is_alive():
                process.terminate()
        kafka_producer.flush(10)
        logger.info('Clean shutdown complete.')


if __name__ == "__main__":
    logger.info('Starting...')
    collect()
