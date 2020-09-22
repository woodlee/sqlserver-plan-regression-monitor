#!/usr/bin/env python3
import datetime
import logging
import re
from datetime import datetime, tzinfo, timezone, timedelta
from typing import Tuple, List

import confluent_kafka
import pyodbc

from . import config, queries


logger = logging.getLogger('plan_monitor.common')


def get_db_conn_with_failover(odbc_conn_string: str, principal_server_key_name: str = 'Server',
                              failover_partner_key_name: str = 'Failover_Partner') -> Tuple[pyodbc.Connection, tzinfo]:
    # FreeTDS doesn't do failover, so we're hacking it in here. This will only work for initial connections. If a
    # failover happens while this process is running, the app will crash. Have a process supervisor that can restart it
    # if that happens, and it'll connect to the new principal on restart. THIS ASSUMES that you are using the exact
    # keywords 'Server' and 'Failover_Partner' (case insensitive) in your connection string!
    try:
        return get_db_conn(odbc_conn_string)
    except pyodbc.ProgrammingError as e:
        server = re.match(rf".*[; ]{principal_server_key_name}=(?P<hostname>.*?);", odbc_conn_string, re.IGNORECASE)
        failover = re.match(rf".*[; ]{failover_partner_key_name}=(?P<hostname>.*?);", odbc_conn_string, re.IGNORECASE)
        if server is None or failover is None or e.args[0] != '42000':
            raise
        server = server.groups('hostname')[0]
        failover = failover.groups('hostname')[0]
        logger.warning('Connection to %s failed; trying failover partner %s ...', server, failover)
        return get_db_conn(odbc_conn_string.replace(server, failover))


def get_db_conn(odbc_conn_string: str) -> Tuple[pyodbc.Connection, tzinfo]:
    conn = pyodbc.connect(odbc_conn_string)
    with conn.cursor() as cursor:
        cursor.execute(queries.CONNECT_METADATA_QUERY)
        db_name, tz_offset, db_now_utc = cursor.fetchone()
    logger.info('Connected to DB %s with timezone offset %s', db_name, tz_offset)
    db_clock_skew = (db_now_utc - datetime.utcnow()).total_seconds()
    if abs(db_clock_skew) > 0.9 * config.MAX_ALLOWED_EVALUATION_LAG_SECONDS:
        raise Exception(f'DB clock skew of {db_clock_skew:.1f} seconds exceeds 90% of the configured '
                        'MAX_ALLOWED_EVALUATION_LAG_SECONDS. Bailing out.')
    elif abs(db_clock_skew) > 0.5 * config.MAX_ALLOWED_EVALUATION_LAG_SECONDS:
        logger.warning(f'DB clock skew of {db_clock_skew:.1f} seconds exceeds 50% of the configured '
                       'MAX_ALLOWED_EVALUATION_LAG_SECONDS.')
    else:
        logger.debug(f'DB clock skew is {db_clock_skew:.1f} seconds.')
    return conn, datetime.strptime(tz_offset, '%z').tzinfo


def kafka_producer_delivery_cb(err: confluent_kafka.KafkaError, msg: confluent_kafka.Message) -> None:
    if err is not None:
        logger.error("Delivery failed for record %s: %s", msg.key(), err)


def format_msg_info(msg: confluent_kafka.Message) -> str:
    return f'Topic {msg.topic()}, partition {msg.partition()}, offset {msg.offset():,}, timestamp ' \
           f'{format_ts(msg.timestamp()[1])}, key {msg.key()}.'


def msg_coordinates(msg: confluent_kafka.Message) -> str:
    return f'{msg.topic()}+{msg.partition()}+{msg.offset()}'


def format_ts(timestamp: int) -> str:
    # converts Unix epoch MILLIseconds integer for human readability
    return datetime.fromtimestamp(timestamp / 1000.0, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


def set_offsets_to_time(start_from_seconds_ago: int, consumer: confluent_kafka.DeserializingConsumer,
                        partitions: List[confluent_kafka.TopicPartition]) -> None:
    start_from = datetime.now(timezone.utc) - timedelta(seconds=start_from_seconds_ago)
    logger.info('Setting consumer offsets to start from %s', start_from)
    for p in partitions:
        p.offset = int(start_from.timestamp() * 1000)  # yep, it's a weird API
    consumer.assign(partitions)
    for p in consumer.offsets_for_times(partitions):
        logger.debug('Topic %s partition %s SEEKing to offset %s', p.topic, p.partition, p.offset)
        consumer.seek(p)
