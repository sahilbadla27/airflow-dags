from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import os


default_args = {
    'owner': 'ross',
    'depends_on_past': False,
    'start_date': datetime(2019, 3, 11),
    'email': ['ross.baehr@earnin.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    }

pipeline_dir = os.environ["AIRFLOW_DAGS_HOME"] + '/reviews_updater'
s3_config_path = os.environ["S3_DAGS_CONFIG_HOME"] + '/reviews_updater'
config_path = pipeline_dir + '/config'
os.environ["AH_CONFIGURATION_PATH"] = config_path
nexus_user= 'ci'
nexus_password= 'CSKNLycd25LfSV5z'


dag = DAG(
        'reviews-updater',
        default_args=default_args,
        schedule_interval='@daily'
        )

t1 = BashOperator(
    task_id='install_packages',
    bash_command='pip3 install --user -r '+pipeline_dir+'/requirements.txt --extra-index-url http://'+nexus_user+':'+nexus_password+'@artifacts.k8s.us-west-2.dev.earnin.com/repository/debug-pypi/simple --trusted-host artifacts.k8s.us-west-2.dev.earnin.com',
    dag=dag)

task = BashOperator(
        task_id='update_reviews',
        bash_command='python ' + pipeline_dir + '/src/update_reviews.py',
        dag=dag
        )
t1 >> task
