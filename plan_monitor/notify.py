#!/usr/bin/env python3
import datetime
import json
import logging
import socket
from datetime import datetime
from typing import Dict, Any

import confluent_kafka
import requests

from jinja2 import Template
from slack import WebClient
from slack.errors import SlackApiError

from . import config, common, message_schemas

logger = logging.getLogger('plan_monitor.notify')


def notify_slack(slack_client: WebClient, msg: Dict[str, Any]) -> None:
    prior_plans_count, prior_times_sum, prior_reads_sum, prior_execs_sum, prior_last_execution = 0, 0, 0, 0, 0
    for pp in msg['prior_plans']:
        prior_plans_count += 1
        prior_times_sum += pp['total_elapsed_time']
        prior_reads_sum += pp['total_logical_reads']
        prior_execs_sum += pp['execution_count']
        prior_last_execution = max(prior_last_execution, pp['last_execution_time'])

    avg_prior_time_ms = prior_times_sum / prior_execs_sum / 1000
    avg_prior_reads = prior_reads_sum / prior_execs_sum
    avg_time_ms = msg['total_elapsed_time'] / msg['execution_count'] / 1000
    avg_reads = msg['total_logical_reads'] / msg['execution_count']
    time_increase_factor = avg_time_ms / avg_prior_time_ms
    read_increase_factor = (avg_reads / avg_prior_reads) if avg_prior_reads else 0
    eviction_latency_seconds = int((msg['eviction_time'] - msg['creation_time']) / 1000)

    template = Template(config.SLACK_MESSAGE_TEMPLATE)
    template.globals['format_ts'] = common.format_ts
    rendered = template.render(msg=msg, prior_plans_count=prior_plans_count, prior_times_sum=prior_times_sum,
                               prior_reads_sum=prior_reads_sum, prior_execs_sum=prior_execs_sum,
                               prior_last_execution=prior_last_execution, avg_prior_time_ms=avg_prior_time_ms,
                               avg_prior_reads=avg_prior_reads, avg_time_ms=avg_time_ms, avg_reads=avg_reads,
                               time_increase_factor=time_increase_factor, read_increase_factor=read_increase_factor,
                               eviction_latency_seconds=eviction_latency_seconds, hostname=socket.getfqdn())

    if slack_client:
        try:
            if config.SLACK_POST_AS_BLOCKS:
                slack_client.chat_postMessage(channel=config.SLACK_NOTIFY_CHANNEL, blocks=json.loads(rendered))
            else:
                slack_client.chat_postMessage(channel=config.SLACK_NOTIFY_CHANNEL, text=rendered)
        except SlackApiError as e:
            logger.warning(f"Error sending message to Slack: {e.response.get('error', '<none>')}")


def notify_http(msg: confluent_kafka.Message) -> None:
    if config.HTTP_NOTIFY_TEMPLATE:
        template = Template(config.HTTP_NOTIFY_TEMPLATE)
        body = template.render(msg=msg)
    else:
        body = json.dumps(msg)

    headers = json.loads(config.HTTP_NOTIFY_HEADERS) if config.HTTP_NOTIFY_HEADERS else {}
    resp = requests.post(config.HTTP_NOTIFY_URL, data=body, headers=headers, timeout=5.0)
    resp.raise_for_status()

    logger.debug('Posted eviction notification to %s with code %s and response: %s', config.HTTP_NOTIFY_URL,
                 resp.status_code, resp.text)


def notify() -> None:
    kafka_consumer = common.build_consumer(f'sqlserver_plan_regression_monitor_notify_{socket.getfqdn()}', True,
                                           message_schemas.EVICTED_PLANS_MESSAGE_KEY_AVRO_SCHEMA,
                                           message_schemas.EVICTED_PLANS_MESSAGE_VALUE_AVRO_SCHEMA)
    kafka_consumer.subscribe([config.EVICTED_PLANS_TOPIC])
    last_log_heartbeat = datetime.utcnow()
    log_heartbeat_interval_seconds = 60
    successive_errors = 0

    if config.SLACK_NOTIFY_CHANNEL:
        slack_client = WebClient(token=config.SLACK_API_TOKEN)
    else:
        slack_client = None

    try:
        while True:
            if (datetime.utcnow() - last_log_heartbeat).total_seconds() > log_heartbeat_interval_seconds:
                positions = kafka_consumer.position(kafka_consumer.assignment())
                logger.info(f'Logging heartbeat: Current Kafka consumer position(s): {positions}')
                last_log_heartbeat = datetime.utcnow()

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

            if slack_client is not None:
                logger.info(f'Notifying Slack channel {config.SLACK_NOTIFY_CHANNEL} for message '
                            f'@ {common.msg_coordinates(msg)}')
                notify_slack(slack_client, dict(msg.value()))

            if config.HTTP_NOTIFY_URL:
                logger.info(f'Notifying via HTTP POST for message '
                            f'@ {common.msg_coordinates(msg)}')
                notify_http(msg)

            last_log_heartbeat = datetime.utcnow()
            kafka_consumer.store_offsets(msg)
    except KeyboardInterrupt:
        logger.info('Received interrupt request; shutting down...')
    finally:
        kafka_consumer.close()
        logger.info('Clean shutdown complete.')


if __name__ == "__main__":
    logger.info('Starting...')
    notify()
