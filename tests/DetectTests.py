import os
import datetime
from unittest import TestCase, mock
from plan_monitor import config
from plan_monitor.detect import calculate_plan_age_stats, is_established_plan, \
    get_query_plan_hashes_under_investigation

def get_time_diff_from_ms(start_time: datetime, seconds_to_subtract: int) -> int:
    ts = start_time - datetime.timedelta(seconds=seconds_to_subtract)
    return ts.timestamp() * 1000
 
class CalculatePlanAge(TestCase):

    def test_calculate_plan_age(self):
        plan_stats = {
            "creation_time": 3234325,
            "last_execution_time": 324242,
            "worst_statement_query_plan_hash": "23424252"
        }
        dt = datetime.datetime.now()
        stats_time = int(dt.strftime("%Y%m%d%H%M%S"))
        plan_age_seconds, last_execution_time_seconds = calculate_plan_age_stats(plan_stats, stats_time)
        expected_plan_age = (stats_time - plan_stats['creation_time']) / 1000
        exected_last_execution_age = (stats_time - plan_stats['last_execution_time']) / 1000
        self.assertEqual(expected_plan_age, plan_age_seconds)
        self.assertEqual(exected_last_execution_age, last_execution_time_seconds)

class IsEstablishedPlan(TestCase):

    # test plan established because plan age is sufficiently old
    @mock.patch('plan_monitor.config')
    def test_is_established_plan_plan_age(self, conf):
        conf.MAX_AGE_OF_LAST_EXECUTION_SECONDS.return_value = 5
        conf.MAX_NEW_PLAN_AGE_SECONDS.return_value = 3
        dt = datetime.datetime.now()
        stats_time = dt.timestamp() * 1000
        established_create_ms = get_time_diff_from_ms(dt, (config.MAX_NEW_PLAN_AGE_SECONDS + 1))
        established_last_execution_ms = get_time_diff_from_ms(dt, (config.MAX_AGE_OF_LAST_EXECUTION_SECONDS - 1))
        plan_stats = {
            "creation_time": established_create_ms,
            "last_execution_time": established_last_execution_ms,
            "worst_statement_query_plan_hash": "23424252"
        }
        
        plan_age_seconds, last_execution_time_seconds = calculate_plan_age_stats(plan_stats, stats_time)
        is_established = is_established_plan(plan_age_seconds, last_execution_time_seconds)
        self.assertTrue(is_established)

    # test plan established because plan execution age is sufficiently old
    @mock.patch('plan_monitor.config')
    def test_is_established_plan_execution_age(self, conf):
        conf.MAX_AGE_OF_LAST_EXECUTION_SECONDS.return_value = 5
        conf.MAX_NEW_PLAN_AGE_SECONDS.return_value = 3
        dt = datetime.datetime.now()
        stats_time = dt.timestamp() * 1000
        established_create_ms = get_time_diff_from_ms(dt, (config.MAX_NEW_PLAN_AGE_SECONDS - 1))
        established_last_execution_ms = get_time_diff_from_ms(dt, (config.MAX_AGE_OF_LAST_EXECUTION_SECONDS + 1))
    
        plan_stats = {
            "creation_time": established_create_ms,
            "last_execution_time": established_last_execution_ms,
            "worst_statement_query_plan_hash": "23424252"
        }
        
        plan_age_seconds, last_execution_time_seconds = calculate_plan_age_stats(plan_stats, stats_time)
        is_established = is_established_plan(plan_age_seconds, last_execution_time_seconds)
        self.assertTrue(is_established)
    
    # test plan established because plan execution age and plan age are sufficiently old
    @mock.patch('plan_monitor.config')
    def test_is_established_plan_both(self, conf):
        conf.MAX_AGE_OF_LAST_EXECUTION_SECONDS.return_value = 5
        conf.MAX_NEW_PLAN_AGE_SECONDS.return_value = 3
        dt = datetime.datetime.now()
        stats_time = dt.timestamp() * 1000
        established_create_ms = get_time_diff_from_ms(dt, (config.MAX_NEW_PLAN_AGE_SECONDS + 1))
        established_last_execution_ms = get_time_diff_from_ms(dt, (config.MAX_AGE_OF_LAST_EXECUTION_SECONDS + 1))
        plan_stats = {
            "creation_time": established_create_ms,
            "last_execution_time": established_last_execution_ms,
            "worst_statement_query_plan_hash": "23424252"
        }
        
        plan_age_seconds, last_execution_time_seconds = calculate_plan_age_stats(plan_stats, stats_time)
        is_established = is_established_plan(plan_age_seconds, last_execution_time_seconds)
        self.assertTrue(is_established)

    @mock.patch('plan_monitor.config')
    def test_is_not_established_plan(self, conf):
        conf.MAX_AGE_OF_LAST_EXECUTION_SECONDS.return_value = 5
        conf.MAX_NEW_PLAN_AGE_SECONDS.return_value = 3
        dt = datetime.datetime.now()
        stats_time = dt.timestamp() * 1000
        established_create_ms = get_time_diff_from_ms(dt, (config.MAX_NEW_PLAN_AGE_SECONDS - 1))
        established_last_execution_ms = get_time_diff_from_ms(dt, (config.MAX_AGE_OF_LAST_EXECUTION_SECONDS - 1))
        plan_stats = {
            "creation_time": established_create_ms,
            "last_execution_time": established_last_execution_ms,
            "worst_statement_query_plan_hash": "23424252"
        }
        
        plan_age_seconds, last_execution_time_seconds = calculate_plan_age_stats(plan_stats, stats_time)
        is_established = is_established_plan(plan_age_seconds, last_execution_time_seconds)
        self.assertFalse(is_established)

class GetActiveQueryPlanHashes(TestCase):

    @mock.patch('plan_monitor.config')
    def test_get_query_plan_hash_under_investigation(self, conf):
        conf.MAX_AGE_OF_LAST_EXECUTION_SECONDS.return_value = 5
        conf.MAX_NEW_PLAN_AGE_SECONDS.return_value = 3
        duplicate_query_plan_hash = '2FCDCA2278D3D2A3'
        dt = datetime.datetime.now()
        stats_time = dt.timestamp() * 1000
        plans = {
            "plan-one-duplicate-qp-hash-not-established": {
                "creation_time": get_time_diff_from_ms(dt, (config.MAX_NEW_PLAN_AGE_SECONDS - 1)),
                "last_execution_time": get_time_diff_from_ms(dt, (config.MAX_AGE_OF_LAST_EXECUTION_SECONDS - 1)),
                "worst_statement_query_plan_hash": duplicate_query_plan_hash
            },
            "not-a-match-established": {
                "creation_time": get_time_diff_from_ms(dt, (config.MAX_NEW_PLAN_AGE_SECONDS + 1)),
                "last_execution_time": get_time_diff_from_ms(dt, (config.MAX_AGE_OF_LAST_EXECUTION_SECONDS - 1)),
                "worst_statement_query_plan_hash": "not-a-query-plan-hash-match"
            },
            "plan-two-duplicate-qp-hash-is-established": {
                "creation_time": get_time_diff_from_ms(dt, (config.MAX_NEW_PLAN_AGE_SECONDS + 2)),
                "last_execution_time": get_time_diff_from_ms(dt, (config.MAX_AGE_OF_LAST_EXECUTION_SECONDS + 2)),
                "worst_statement_query_plan_hash": duplicate_query_plan_hash
            }
        }
        
        qp_hashes_under_investigation = get_query_plan_hashes_under_investigation(plans, stats_time)
        self.assertEqual(len(qp_hashes_under_investigation), 1)
        self.assertEqual(qp_hashes_under_investigation.pop(), duplicate_query_plan_hash)


    @mock.patch('plan_monitor.config')
    def test_get_query_plan_hash_under_investigation_returns_qp_hash(self, conf):
        conf.MAX_AGE_OF_LAST_EXECUTION_SECONDS.return_value = 5
        conf.MAX_NEW_PLAN_AGE_SECONDS.return_value = 3
        duplicate_query_plan_hash = '2FCDCA2278D3D2A3'
        dt = datetime.datetime.now()
        stats_time = dt.timestamp() * 1000
        plans = {
            "plan-one-duplicate-qp-hash-not-established": {
                "creation_time": get_time_diff_from_ms(dt, (config.MAX_NEW_PLAN_AGE_SECONDS - 1)),
                "last_execution_time": get_time_diff_from_ms(dt, (config.MAX_AGE_OF_LAST_EXECUTION_SECONDS - 1)),
                "worst_statement_query_plan_hash": duplicate_query_plan_hash
            },
            "not-a-match-established": {
                "creation_time": get_time_diff_from_ms(dt, (config.MAX_NEW_PLAN_AGE_SECONDS + 1)),
                "last_execution_time": get_time_diff_from_ms(dt, (config.MAX_AGE_OF_LAST_EXECUTION_SECONDS - 1)),
                "worst_statement_query_plan_hash": "not-a-query-plan-hash-match"
            },
            "plan-two-duplicate-qp-hash-not-established-either": {
                "creation_time": get_time_diff_from_ms(dt, (config.MAX_NEW_PLAN_AGE_SECONDS - 1)),
                "last_execution_time": get_time_diff_from_ms(dt, (config.MAX_AGE_OF_LAST_EXECUTION_SECONDS - 1)),
                "worst_statement_query_plan_hash": duplicate_query_plan_hash
            }
        }
        
        qp_hashes_under_investigation = get_query_plan_hashes_under_investigation(plans, stats_time)
        self.assertEqual(len(qp_hashes_under_investigation), 1)
        self.assertEqual(qp_hashes_under_investigation.pop(), duplicate_query_plan_hash)

    @mock.patch('plan_monitor.config')
    def test_get_query_plan_hash_under_investigation_doesnt_return_established_plans(self, conf):
        conf.MAX_AGE_OF_LAST_EXECUTION_SECONDS.return_value = 5
        conf.MAX_NEW_PLAN_AGE_SECONDS.return_value = 3
        duplicate_query_plan_hash = '2FCDCA2278D3D2A3'
        dt = datetime.datetime.now()
        stats_time = dt.timestamp() * 1000
        plans = {
            "plan-one-duplicate-qp-hash-established": {
                "creation_time": get_time_diff_from_ms(dt, (config.MAX_NEW_PLAN_AGE_SECONDS + 1)),
                "last_execution_time": get_time_diff_from_ms(dt, (config.MAX_AGE_OF_LAST_EXECUTION_SECONDS + 1)),
                "worst_statement_query_plan_hash": duplicate_query_plan_hash
            },
            "not-a-match-established": {
                "creation_time": get_time_diff_from_ms(dt, (config.MAX_NEW_PLAN_AGE_SECONDS + 1)),
                "last_execution_time": get_time_diff_from_ms(dt, (config.MAX_AGE_OF_LAST_EXECUTION_SECONDS - 1)),
                "worst_statement_query_plan_hash": "not-a-query-plan-hash-match"
            },
            "plan-two-duplicate-qp-hash-established-either": {
                "creation_time": get_time_diff_from_ms(dt, (config.MAX_NEW_PLAN_AGE_SECONDS + 1)),
                "last_execution_time": get_time_diff_from_ms(dt, (config.MAX_AGE_OF_LAST_EXECUTION_SECONDS + 1)),
                "worst_statement_query_plan_hash": duplicate_query_plan_hash
            }
        }
        
        qp_hashes_under_investigation = get_query_plan_hashes_under_investigation(plans, stats_time)
        self.assertEqual(len(qp_hashes_under_investigation), 0)


