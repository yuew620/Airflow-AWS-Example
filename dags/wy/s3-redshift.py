# import modules/libraries
from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators import glue_crawler
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
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
    # add redshift connection first
    # edshift_conn_id: str = 'redshift_default'
    task_transfer_s3_to_redshift = S3ToRedshiftOperator(
        s3_bucket='lab-519201465192-sin-com',
        s3_key='spark/output2',
        schema='PUBLIC',
        table='table1',
        copy_options=['parquet'],
        task_id='transfer_s3_to_redshift',
    )


# call the EMR steps.
    task_transfer_s3_to_redshift
