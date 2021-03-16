from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(dag)
    print(task_list)
    print(blocking_task_list)
    print(slas)
    print(blocking_tis)


default_args = {
    'email': 'adil.khashtamov@gmail.com',
    'sla': timedelta(seconds=5),
}

with DAG(
    dag_id='sla_example_dag',
    sla_miss_callback=sla_miss_callback,
    start_date=datetime(2021, 3, 16),
    schedule_interval='*/5 * * * *',
    catchup=False,
    default_args=default_args,
) as dag:

    cmd = BashOperator(
        task_id='slow_task',
        bash_command='sleep 15',
        dag=dag,
    )
