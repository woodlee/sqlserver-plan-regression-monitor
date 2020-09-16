import json
import os


# Configuration that is defaulted for local development that you should probably override for deployment:
# ------------------------------------------------------------------------------------------------------------

# Kafka things:
SCHEMA_REGISTRY_URL = os.environ.get('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
STATS_TOPIC = os.environ.get('STATS_TOPIC', 'sqlserver_plan_regression_monitor_stats')
BAD_PLANS_TOPIC = os.environ.get('BAD_PLANS_TOPIC', 'sqlserver_plan_regression_monitor_bad_plans')
EVICTED_PLANS_TOPIC = os.environ.get('EVICTED_PLANS_TOPIC', 'sqlserver_plan_regression_monitor_evicted_plans')

# This should be a JSON object whose keys are a string identifier of your choosing for the DB, and the values
# are a corresponding ODBC connection string:
ODBC_CONN_STRINGS = json.loads(os.environ.get('ODBC_CONN_STRINGS', '''
{"LOCAL": "DRIVER=FreeTDS; Server=localhost; Database=MyDB; UID=sa; PWD=1Password; Port=1433; App=plan_monitor;"}'''))


# Configuration with sane defaults for most deployments but that you can change if you wish:
# ------------------------------------------------------------------------------------------------------------

AVRO_SCHEMA_NAMESPACE = os.environ.get('AVRO_SCHEMA_NAMESPACE', 'sqlserver_plan_regression_monitor')
DB_STATS_POLL_INTERVAL_SECONDS = int(os.environ.get('DB_STATS_POLL_INTERVAL_SECONDS', 30))

# Controls page size for fetchmany() calls on the stats DB query result set:
STATS_ROW_FETCH_SIZE = int(os.environ.get('STATS_ROW_FETCH_SIZE', 1000))

# On this interval the "detect" module will purge its current accumulated stats history and then reload this many
# minutes' worth thereof from Kafka. It also controls how much history is read from Kafka on initial startup of
# "detect", or from the DB on initial startup of "collect". Sort of a hacky mechanism for preventing long-unused plans
# from bloating in-memory data structures:
REFRESH_INTERVAL_MINUTES = int(os.environ.get('REFRESH_INTERVAL_MINUTES', 60 * 4))  # 4 hours

# If messages being read out of Kafka are older than this compared to the system clock, they will still be used for
# gathering stats history, but associated plans will not be considered bad. NOTE: this may be affected by clock skew!!
MAX_ALLOWED_EVALUATION_LAG_SECONDS = int(os.environ.get('MAX_ALLOWED_EVALUATION_LAG_SECONDS', 120))


# Bad plan detection tuning
# ------------------------------------------------------------------------------------------------------------
# NOTE: Plan ages are calculated relative to when the stats were gathered from SQL Server, which may be up
# to MAX_ALLOWED_EVALUATION_LAG_SECONDS behind "now".
# All values should be integers.

# To be considered bad, a new plan must be at least this old:
MIN_NEW_PLAN_AGE_SECONDS = int(os.environ.get('MIN_NEW_PLAN_AGE_SECONDS', 60))

# Plans older than this will be considered "established" and never considered bad:
MAX_NEW_PLAN_AGE_SECONDS = int(os.environ.get('MAX_NEW_PLAN_AGE_SECONDS', MIN_NEW_PLAN_AGE_SECONDS * 10))  # 600

# A bad plan must have amassed at least this much elapsed query time OR at least this many total logical reads across
# all its executions so far:
MIN_TOTAL_ELAPSED_TIME_SECONDS = int(os.environ.get('MIN_TOTAL_ELAPSED_TIME_SECONDS', 60 * 10))  # 10 minutes
MIN_TOTAL_LOGICAL_READS = int(os.environ.get('MIN_TOTAL_LOGICAL_READS', 1_000_000))

# Compared to all prior plans used, a new plan may be considered bad if its average execution time OR its average
# number of reads per execution increases by the factor specified. An increase in reads will not flag the plan as
# bad if the new plan uses LESS average time:
MIN_TIME_INCREASE_FACTOR = int(os.environ.get('MIN_TIME_INCREASE_FACTOR', 10))
MIN_READS_INCREASE_FACTOR = int(os.environ.get('MIN_READS_INCREASE_FACTOR', 10))

# A plan will not be considered bad if its last execution was longer than this many seconds ago (on the presumption
# that it has already been replaced with a newer plan):
MAX_AGE_OF_LAST_EXECUTION_SECONDS = int(os.environ.get('MAX_AGE_OF_LAST_EXECUTION_SECONDS', 30))

# A plan will not be considered bad until it has been executed this many times OR it has reached this age. Furthermore,
# executions under all other plans summed together must reach the count threshold for them to be considered as
# providing "reliable data" for comparison:
MIN_EXECUTION_COUNT = int(os.environ.get('MIN_EXECUTION_COUNT', 250))
MIN_AGE_IN_LIEU_OF_EXEC_COUNT_SECONDS = int(os.environ.get(
    'MIN_AGE_IN_LIEU_OF_EXEC_COUNT_SECONDS', MAX_NEW_PLAN_AGE_SECONDS - MIN_NEW_PLAN_AGE_SECONDS))  # 540
