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
AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]
BUILDS_PATH = AIRFLOW_HOME + '/builds'
AIRFLOW_DAGS_HOME = os.environ["AIRFLOW_DAGS_HOME"]

dag = DAG(
    's3_sync_packages', default_args=default_args, schedule_interval='@once')

t1 = BashOperator(
    task_id='aws_profile',
    bash_command='export AWS_PROFILE=stellarole ',
    dag=dag)

t2 = BashOperator(
    task_id='aws_config',
    bash_command='export AWS_CONFIG_FILE={{ params.loc }}',
    params={'loc': '~/.aws/config'},
    dag=dag)

aws_task = BashOperator(
    task_id='aws_install',
    bash_command='pip install --upgrade --user awscli && export PATH=~/.local/bin:$PATH',
    dag=dag)

sync_task = BashOperator(
    task_id='run_s3_sync',
    bash_command='mkdir -p {{ params.source }} && aws s3 sync {{ params.source }} {{ params.dest }}',
    params={'dest': BUILDS_PATH, 'source': S3_DAGS_HOME},
    dag=dag
)

untar_task = BashOperator(
    task_id='untar_package',
    bash_command='cd {{ params.source }} && for f in *.tar; do tar xf $f -C {{ params.dest }}; done',
    params={'dest': AIRFLOW_DAGS_HOME, 'source': BUILDS_PATH},
    dag=dag
)

clean_task = BashOperator(
    task_id='clean_tar_files',
    bash_command='rm -r {{ params.source }}/*',
    params={'source': BUILDS_PATH},
    dag=dag
)

t1 >> t2 >> aws_task >> sync_task >> untar_task >> clean_task
