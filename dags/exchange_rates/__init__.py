from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from .operator import CurrencyScoopOperator


with DAG(
        dag_id='exchange_rate_usd_kzt_dag',
        start_date=datetime(2021, 3, 1),
        schedule_interval='@daily',
) as dag:

    create_table = PostgresOperator(
        task_id='create_table_task',
        sql='sql/create_table.sql',
        postgres_conn_id='postgres_default',
    )

    get_rate = CurrencyScoopOperator(
        task_id='get_rate',
        base_currency='USD',
        currency='KZT',
        conn_id='cur_scoop_conn_id',
        dag=dag,
        do_xcom_push=True,
    )

    insert_rate = PostgresOperator(
        task_id='insert_rate',
        postgres_conn_id='postgres_default',
        sql='sql/insert_rate.sql',
        params={
            'base_currency': 'USD',
            'currency': 'KZT',
        }
    )

    create_table >> get_rate >> insert_rate
