import datetime as dt

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2021, 1, 20),
}


@dag(
    default_args=default_args,
    schedule_interval='@daily',
    dag_id='taskflow_dag_with_two_operators'
)
def first_dag_taskflow():
    @task
    def even_only():
        context = get_current_context()
        execution_date = context['execution_date']

        if execution_date.day % 2 != 0:
            raise ValueError(f'Odd day: {execution_date}')

    @task
    def dummy_task():
        pass

    even_only() >> dummy_task()

    # вариант с Operator из Airflow
    # even_only() >> DummyOperator(task_id='dummy_task')


main_dag = first_dag_taskflow()
