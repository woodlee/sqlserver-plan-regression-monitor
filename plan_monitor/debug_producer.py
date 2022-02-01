import logging
import pprint

logger = logging.getLogger('plan_monitor.debug_producer')


class DebugKafkaProducer(object):
    def flush(self, *args) -> None:
        pass

    def poll(self, *args) -> None:
        pass

    # noinspection PyMethodMayBeStatic
    def produce(self, topic, key, value) -> None:
        logger.debug('DEBUG MODE - no Kafka produce done. Would have been: Topic: %s, Key: %s, Value: %s',
                     topic, pprint.pformat(key), pprint.pformat(value))
