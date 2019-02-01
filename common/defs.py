# Definitions used by the services and their API

TASK_COUNT = '_task_count'
DONE_TASK_COUNT = '_done_task_count'
TASK_SUFFIX = '_task'
TASK_HANDLER_SUFFIX = '_queue'
EXEC_TIME_INFO  = 'exec_time_info'

NANO_SEC_ADJUSTMENT = 1000000000
EXPECTED_TIME_RANGE = NANO_SEC_ADJUSTMENT ** 2

REDIS_URL = 'redis://redis:6379/0'
R_TASKS_LIST          = "{}.tasks"
R_TASK_INFO           = "{}.task.{}"
R_TASK_TYPE_ASSIGNED  = "{}.task_type.{}.total_count"
R_TASK_TYPE_TOTAL     = "{}.task_type.{}.total_count"
R_TASK_TYPE_DONE      = "{}.task_type.{}.done_count"

