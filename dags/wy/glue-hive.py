# import modules/libraries
from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators import glue_crawler
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator
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
# SPARK_TASK = [
#     {
#         'Name': 'glue-spark-redshift',
#         'ActionOnFailure': 'CONTINUE',
#         'HadoopJarStep': {
#             "Jar": "command-runner.jar",
#             "Args": ["/usr/lib/spark/bin/run-example", "SparkPi", "10"],
#         },
#     }
# ]

SPARK_TASK = [
    {
        "Name": "sparks3tos3",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "--master",
                "yarn",
                "--conf",
                "spark.yarn.submit.waitAppCompletion=true",
                "--class",
                "com.demo.emr.spark.Lab2",
                "s3://lab-519201465192-sin-com/spark/original-emr-0.0.1-SNAPSHOT.jar",
                "s3://lab-519201465192-sin-com/spark/output2",
            ],
        },
    }
]

# set the variables.
DAG_ID = os.path.basename(__file__).replace(".py", "")
cluster_id = "j-2N7CB5LUECF6K"
glue_job_name = "glue-to-s3"
region_name = "ap-southeast-1"
glue_iam_role = "AWSGlueServiceRole"

with DAG(
        dag_id=DAG_ID,
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(hours=2),
        start_date=days_ago(1),
        schedule_interval=None,
        tags=['pyspark'],
) as dag:
    # glue etl job is added in glue console
    # here just trigger the glue etl job to run
    # the key is job_name
    glue_job_step = AwsGlueJobOperator(
        task_id="glue_job_step",
        job_name=glue_job_name,
        job_desc="triggering glue job {glue_job_name}",
        region_name=region_name,
        iam_role_name=glue_iam_role,
        num_of_dpus=1,
        aws_conn_id='aws_default',
        dag=dag
    )
    # the glue crawler is added in glue console
    # here , just trigger the craler to run
    # the key is Name
    glue_crawler_job_step = AwsGlueCrawlerOperator(
        config={'Name': 'lab2-crawler-glue-rds'},
        task_id='glue_crawler_emr_step'
    )
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
    glue_job_step >> glue_crawler_job_step >> step_first >> step_second
