from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from .operator import CurrencyScoopOperator


with DAG(
        dag_id='exchange_rate_dynamic',
        start_date=datetime(2021, 3, 1),
        schedule_interval='@daily',
        catchup=False,
) as dag:

    create_table = PostgresOperator(
        task_id='create_table_task',
        sql='sql/create_table.sql',
        postgres_conn_id='postgres_default',
    )

    tasks = []

    for base, currency in [
        ('USD', 'KZT'),
        ('USD', 'RUB'),
        ('USD', 'EUR'),
        ('KZT', 'RUB'),
        ('RUB', 'KZT'),
        ('EUR', 'KZT'),
        ('EUR', 'RUB'),
    ]:
        get_rate_task = CurrencyScoopOperator(
            task_id=f'get_rate_{ base }_{ currency }',
            base_currency=base,
            currency=currency,
            conn_id='cur_scoop_conn_id',
            dag=dag,
            do_xcom_push=True,
        )

        insert_rate = PostgresOperator(
            task_id=f'insert_rate_{ base }_{ currency }',
            postgres_conn_id='postgres_default',
            sql='sql/insert_rate.sql',
            params={
                'base_currency': base,
                'currency': currency,
                'get_rate_task_id': f'get_rate_{ base }_{ currency }'
            }
        )

        get_rate_task >> insert_rate

        tasks.append(get_rate_task)

    create_table.set_downstream(tasks)
