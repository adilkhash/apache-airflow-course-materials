import datetime as dt

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.http.sensors.http import HttpSensor

from nyc_taxi.functions import download_dataset, convert_to_parquet

default_args = {
    'owner': 'airflow',
}


with DAG(
    start_date=dt.datetime(2021, 1, 1),
    dag_id='nyc_taxi_2021_dag',
    schedule_interval='@monthly',
    default_args=default_args,
) as dag:

    check_if_exists = HttpSensor(
        method='HEAD',
        endpoint='yellow_tripdata_{{ execution_date.strftime("%Y-%m") }}.csv',
        http_conn_id='nyc_yellow_taxi_id',
        task_id='check_if_exists',
        poke_interval=60 * 60 * 24,  # every 24 hours
        mode='reschedule',
    )

    @task
    def download_file():
        context = get_current_context()
        return download_dataset(context['execution_date'].strftime('%Y-%m'))

    @task
    def to_parquet(file_path: str):
        context = get_current_context()
        return convert_to_parquet(context['execution_date'].strftime('%Y-%m'), file_path)

    file_path = download_file()
    parquet_file_path = to_parquet(file_path)

    check_if_exists >> file_path
