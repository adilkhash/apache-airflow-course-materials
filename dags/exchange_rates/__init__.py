from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from .operator import CurrencyScoopOperator


def print_func(**context):
    ti = context['ti']
    currency_rate = ti.xcom_pull(task_ids='get_rate')
    print(currency_rate)


with DAG(
        dag_id='exchange_rate_usd_kzt_dag',
        start_date=datetime(2021, 3, 1),
        schedule_interval='@daily',
) as dag:

    scoop = CurrencyScoopOperator(
        task_id='get_rate',
        base_currency='USD',
        symbol='KZT',
        conn_id='cur_scoop_conn_id',
        dag=dag,
        do_xcom_push=True,
    )

    print_operator = PythonOperator(
        task_id='print_currency_rate',
        python_callable=print_func,
        dag=dag,
    )

    scoop >> print_operator
