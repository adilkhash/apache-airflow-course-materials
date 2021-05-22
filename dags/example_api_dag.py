import datetime as dt

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2021, 1, 20),
}

with DAG(
    dag_id='example_api_dag',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
) as dag:
    dice = DummyOperator(
        task_id='dummy_task',
        dag=dag,
    )
