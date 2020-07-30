"""
A maintenance workflow that you can deploy into Airflow to periodically clean
out the scheduler logs to avoid those getting too big.
airflow trigger_dag --conf '{"maxLogAgeInDays":30}' airflow-log-cleanup
--conf options:
    maxLogAgeInDays:<INT> - Optional

This DAG is modified version of
https://github.com/teamclairvoyant/airflow-maintenance-dags/blob/master/log-cleanup/airflow-log-cleanup.py
"""
from airflow.models import DAG, Variable
from airflow.configuration import conf
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta
import os
import logging
import airflow


DAG_ID = "airflow-log-cleanup"
START_DATE = airflow.utils.dates.days_ago(1)
CHILD_PROCESS_LOG_DIRECTORY = conf.get("scheduler", "CHILD_PROCESS_LOG_DIRECTORY")
SCHEDULE_INTERVAL = "@daily"
DAG_OWNER_NAME = "operations"
ALERT_EMAIL_ADDRESSES = []
# Length to retain the log files if not already provided in the conf. If this
# is set to 30, the job will remove those files that are 30 days old or older
DEFAULT_MAX_LOG_AGE_IN_DAYS = Variable.get(
    "airflow_log_cleanup__max_log_age_in_days", 14
)

if not CHILD_PROCESS_LOG_DIRECTORY or CHILD_PROCESS_LOG_DIRECTORY.strip() == "":
    raise ValueError(
        "CHILD_PROCESS_LOG_DIRECTORY variable is empty in airflow.cfg. It can be found "
        "under the [scheduler] section in the cfg file. Kindly provide an "
        "appropriate directory path."
    )

default_args = {
    'owner': DAG_OWNER_NAME,
    'depends_on_past': False,
    'email': ALERT_EMAIL_ADDRESSES,
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': START_DATE,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    is_paused_upon_creation=False
)
dag.doc_md = """
Airflow produces quite a lot of log files, and the log pvc gets full fairly easily, 
which in turn prevents the whole application from working. This is why this DAG that removes 
old log files is added and enabled by default. 
**It is strongly encouraged to keep this DAG enabled!**

By default log files get removed after two weeks, but you can define when log files get 
removed by either modifying the DAG directly or creating a variable in the web UI (Admin -> Variables):

* Key: airflow\_log\_cleanup\_\_max\_log\_age\_in\_days
* Value: number of days after a log file is deleted, for example 30

You can manually trigger individual DAG runs with different number of days as configuration by setting
maxLogAgeInDays (for example {"maxLogAgeInDays":30}) as the DAG run configuration JSON.
"""

log_cleanup = """
echo "Getting Configurations..."
CHILD_PROCESS_LOG_DIRECTORY="{{params.directory}}"

MAX_LOG_AGE_IN_DAYS="{{dag_run.conf.maxLogAgeInDays}}"
if [ "${MAX_LOG_AGE_IN_DAYS}" == "" ]; then
    echo "maxLogAgeInDays conf variable isn't included. Using Default '""" + str(DEFAULT_MAX_LOG_AGE_IN_DAYS) + """'."
    MAX_LOG_AGE_IN_DAYS='""" + str(DEFAULT_MAX_LOG_AGE_IN_DAYS) + """'
fi
echo "Finished Getting Configurations"
echo ""

echo "Configurations:"
echo "CHILD_PROCESS_LOG_DIRECTORY:  '${CHILD_PROCESS_LOG_DIRECTORY}'"
echo "MAX_LOG_AGE_IN_DAYS:          '${MAX_LOG_AGE_IN_DAYS}'"

cleanup() {
    echo "Executing Find Statement: $1"
    FILES_MARKED_FOR_DELETE=`eval $1`
    echo "Process will be Deleting the following File(s)/Directory(s):"
    echo "${FILES_MARKED_FOR_DELETE}"
    echo "Process will be Deleting `echo "${FILES_MARKED_FOR_DELETE}" | \
    grep -v '^$' | wc -l` File(s)/Directory(s)"     \
    # "grep -v '^$'" - removes empty lines.
    # "wc -l" - Counts the number of lines
    echo ""
    if [ "${FILES_MARKED_FOR_DELETE}" != "" ];
    then
        echo "Executing Delete Statement: $2"
        eval $2
        DELETE_STMT_EXIT_CODE=$?
        if [ "${DELETE_STMT_EXIT_CODE}" != "0" ]; then
            echo "Delete process failed with exit code \
                '${DELETE_STMT_EXIT_CODE}'"
            exit ${DELETE_STMT_EXIT_CODE}
        fi
    else
        echo "WARN: No File(s)/Directory(s) to Delete"
    fi

}

echo ""
echo "Running Cleanup Process..."

FIND_STATEMENT="find ${CHILD_PROCESS_LOG_DIRECTORY}/*/* -type f -mtime +${MAX_LOG_AGE_IN_DAYS}"
DELETE_STMT="${FIND_STATEMENT} -exec rm -f {} \;"

cleanup "${FIND_STATEMENT}" "${DELETE_STMT}"
CLEANUP_EXIT_CODE=$?

FIND_STATEMENT="find ${CHILD_PROCESS_LOG_DIRECTORY}/* -type d -empty"
DELETE_STMT="${FIND_STATEMENT} -prune -exec rm -rf {} \;"

cleanup "${FIND_STATEMENT}" "${DELETE_STMT}"
CLEANUP_EXIT_CODE=$?

echo "Finished Running Cleanup Process"
"""

log_cleanup_op = BashOperator(
    task_id='log_cleanup',
    bash_command=log_cleanup,
    params={"directory": CHILD_PROCESS_LOG_DIRECTORY},
    dag=dag)
