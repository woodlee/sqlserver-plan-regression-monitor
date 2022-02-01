import collections
import json
import logging
from typing import Dict, Any, Tuple, List

from . import config


logger = logging.getLogger('plan_monitor.models')

PURGE_PLANS_THRESHOLD = 40
CLEAN_MAX_PLAN_AGE_SECONDS = min(config.REFRESH_INTERVAL_MINUTES * 60 / 2, config.MAX_NEW_PLAN_AGE_SECONDS * 8)


class PlanStats(object):
    __slots__ = ["plan_handle", "creation_time", "last_execution_time", "stats_query_time", "statement_count",
                 "execution_count", "total_elapsed_time", "total_logical_reads", "total_logical_writes",
                 "total_worker_time", "worst_statement_query_hash", "worst_statement_query_plan_hash",
                 "worst_statement_start_offset", "source_stats_message_coordinates"]

    def __init__(self, plan_handle: str, creation_time: int, last_execution_time: int, stats_query_time: int,
                 statement_count: int, execution_count: int, total_elapsed_time: int, total_logical_reads: int,
                 total_logical_writes: int, total_worker_time: int, worst_statement_query_hash: str,
                 worst_statement_query_plan_hash: str, worst_statement_start_offset: int,
                 source_stats_message_coordinates: str) -> None:
        self.plan_handle: str = plan_handle
        self.creation_time: int = creation_time
        self.last_execution_time: int = last_execution_time
        self.stats_query_time: int = stats_query_time
        self.statement_count: int = statement_count
        self.execution_count: int = execution_count
        self.total_elapsed_time: int = total_elapsed_time
        self.total_logical_reads: int = total_logical_reads
        self.total_logical_writes: int = total_logical_writes
        self.total_worker_time: int = total_worker_time
        self.worst_statement_query_hash: str = worst_statement_query_hash
        self.worst_statement_query_plan_hash: str = worst_statement_query_plan_hash
        self.worst_statement_start_offset: int = worst_statement_start_offset
        self.source_stats_message_coordinates: str = source_stats_message_coordinates

    def to_dict(self):
        return {x: getattr(self, x) for x in self.__slots__}

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=2)


class PlanStatsStore(object):
    __slots__ = ["_queries"]

    def __init__(self) -> None:
        self._queries: Dict[Tuple[str, int, str], Dict[str, PlanStats]] = collections.defaultdict(dict)

    def __len__(self) -> int:
        return len(self._queries)

    def register_stats_from_message(self, msg: Dict[str, Any],
                                    msg_coordinates: str) -> List[PlanStats]:
        query_key = (msg["db_identifier"], msg["set_options"], msg["sql_handle"])
        current_stats = PlanStats(
            msg["plan_handle"], msg['creation_time'], msg['last_execution_time'], msg['stats_query_time'],
            msg['statement_count'], msg['execution_count'], msg['total_elapsed_time'], msg['total_logical_reads'],
            msg['total_logical_writes'], msg['total_worker_time'], msg['worst_statement_query_hash'],
            msg['worst_statement_query_plan_hash'], msg['worst_statement_start_offset'], msg_coordinates
        )
        if len(self._queries[query_key]) > PURGE_PLANS_THRESHOLD:
            self._clean_plans(query_key)
        self._queries[query_key][msg["plan_handle"]] = current_stats
        return list(self._queries[query_key].values())

    def _clean_plans(self, query_key: Tuple[str, int, str]) -> None:
        keep: List[PlanStats] = []
        before = len(self._queries[query_key])
        for stats in self._queries[query_key].values():
            plan_age_secs = (stats.stats_query_time - stats.creation_time) / 1000
            if plan_age_secs < CLEAN_MAX_PLAN_AGE_SECONDS:
                keep.append(stats)
        self._queries[query_key] = {s.plan_handle: s for s in keep}
        after = len(keep)
        logger.debug('Cleaned %s entries for key %s (before: %s, after %s)', str(before - after), query_key,
                     before, after)
