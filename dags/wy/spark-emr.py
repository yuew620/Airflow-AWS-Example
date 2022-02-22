# import modules/libraries
from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta
import os

# setting up default args
DEFAULT_ARGS = {
    'owner': 'wy',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

# add & edit the args as per your requirement. Change the pyspark file name.
SPARK_TASK = [
    {
        'Name': 'spark_app',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            "Jar": "command-runner.jar",
            "Args": ["/usr/lib/spark/bin/run-example", "SparkPi", "10"],
        },
    }
]

# set the variables.
DAG_ID = os.path.basename(__file__).replace(".py", "")
cluster_id = "j-2N7CB5LUECF6K"


with DAG(
        dag_id=DAG_ID,
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(hours=2),
        start_date=days_ago(1),
        schedule_interval=None,
        tags=['pyspark'],
) as dag:

    # first step to register EMR step
    step_first = EmrAddStepsOperator(
        task_id='add_emr_step',
        job_flow_id=cluster_id,
        aws_conn_id='aws_default',
        steps=SPARK_TASK,
    )

    # second step to keep track of previous step.
    step_second = EmrStepSensor(
        task_id='watch_emr_step',
        job_flow_id=cluster_id,
        step_id="{{ task_instance.xcom_pull(task_ids='add_emr_step', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

    # call the EMR steps.
    step_first >> step_second