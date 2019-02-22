import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os
import sys

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 8, 4, 13, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

S3_DAGS_HOME= os.environ["S3_DAGS_HOME"]
AIRFLOW_DAGS_HOME = os.environ["AIRFLOW_DAGS_HOME"]

dag = DAG(
    's3_sync', default_args=default_args, schedule_interval='@once')

t1 = BashOperator(
    task_id='aws_profile',
    bash_command='export AWS_PROFILE=airflow ',
    dag=dag)

t2 = BashOperator(
    task_id='aws_config',
    bash_command='export AWS_CONFIG_FILE={{ params.loc }}',
    params={'loc': '~/.aws/config'},
    dag=dag)

sync_task = BashOperator(
    task_id='run_s3_sync',
    bash_command='aws s3 sync {{ params.source }} {{ params.dest }} --exclude \'*\' --include \"*.py\"',
    params={'dest': AIRFLOW_DAGS_HOME, 'source': S3_DAGS_HOME},
    dag=dag
)

t1 >> t2 >> sync_task
