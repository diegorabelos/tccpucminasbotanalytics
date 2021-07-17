from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from configparser import RawConfigParser

date = datetime.now()

# Following are defaults which can be overridden later on
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(date.year, date.month, date.day),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '0 9 * * *',
}

dag = DAG('dag_load_data', default_args=default_args)

t1 = BashOperator(
    task_id='task_1',
    bash_command='python3 $AIRFLOW_HOME/dags/get_data_pandemics.py',
    dag=dag)

t2 = BashOperator(
    task_id='task_2',
    bash_command='python3 $AIRFLOW_HOME/dags/get_data_twitter.py',
    dag=dag)

t2.set_upstream(t1)

