import sys
import os
from datetime import datetime

sys.path.append(
    os.path.join(os.path.dirname(__file__), '..'),
)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

from nyc_taxi.functions import download_dataset, convert_to_parquet


def to_parquet(**context):
    ti = context['ti']
    execution_date = context['execution_date']
    s3_path = ti.xcom_pull(task_ids='download_file')
    convert_to_parquet(execution_date.strftime('%Y-%m'), s3_path)


with DAG(
        dag_id='nyc_taxi_pre_2',
        start_date=datetime(2020, 1, 1),
        schedule_interval='@monthly'
) as dag:
    check_file_task = SimpleHttpOperator(
        task_id='check_file',
        method='HEAD',
        http_conn_id='nyc_yellow_taxi_id',
        endpoint='yellow_tripdata_{{ execution_date.strftime("%Y-%m") }}.csv'
    )

    download_file_task = PythonOperator(
        task_id='download_file',
        python_callable=download_dataset,
        op_args=['{{ execution_date.strftime("%Y-%m") }}']
    )

    to_parquet_task = PythonOperator(
        task_id='to_parquet',
        python_callable=to_parquet,
    )

    check_file_task >> download_file_task >> to_parquet_task
