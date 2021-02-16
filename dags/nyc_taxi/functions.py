import datetime as dt
from tempfile import NamedTemporaryFile

import requests
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def download_dataset(year_month: str):
    url = (
        f'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_{year_month}.csv'
    )
    response = requests.get(url, stream=True)
    response.raise_for_status()

    s3 = S3Hook('minio_id')

    s3_path = f's3://yellow-taxi-raw/yellow_tripdata_{year_month}.csv'
    bucket, key = s3.parse_s3_url(s3_path)

    with NamedTemporaryFile('w', encoding='utf-8', delete=False) as f:
        for chunk in response.iter_lines():
            f.write('{}\n'.format(chunk.decode('utf-8')))
    s3.load_file(f.name, key, bucket, replace=True)

    return s3_path


def convert_to_parquet(year_month: str, s3_path: str):
    s3 = S3Hook('minio_id')
    bucket, key = s3.parse_s3_url(s3_path)
    file_path = s3.download_file(key, bucket)
    df = pd.read_csv(file_path)

    current_month = dt.datetime.strptime(year_month, '%Y-%m')
    next_month = current_month.replace(month=current_month.month + 1)

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df = df[
        (df['tpep_pickup_datetime'] >= f'{current_month:%Y-%m-%d}') &
        (df['tpep_pickup_datetime'] < f'{next_month:%Y-%m-%d}')
        ]
    df['pickup_date'] = df['tpep_pickup_datetime'].dt.strftime('%Y-%m-%d')

    s3_path = f's3://yellow-taxi-parquet/yellow_tripdata_{year_month}.parquet'

    with NamedTemporaryFile('wb', delete=False) as f:
        df.to_parquet(f)

    s3.load_file(
        f.name,
        f'yellow_tripdata_{year_month}.parquet',
        'yellow-taxi-parquet', replace=True
    )
    return s3_path
