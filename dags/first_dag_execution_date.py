import datetime as dt

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2021, 1, 20),
}


def even_only():
    context = get_current_context()
    execution_date = context['execution_date']

    if execution_date.day % 2 != 0:
        raise ValueError(f'Odd day: {execution_date}')


with DAG(dag_id='first_dag_execution_date',
         schedule_interval='@daily',
         default_args=default_args) as dag:

    even_only = PythonOperator(
        task_id='even_only',
        python_callable=even_only,
        dag=dag,
    )
