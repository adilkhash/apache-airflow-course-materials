import os
import datetime as dt

import requests
import pandas as pd


def download_dataset(year_month: str):
    url = (
        f'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_{year_month}.csv'
    )
    response = requests.get(url, stream=True)
    response.raise_for_status()

    fpath = os.path.join(
        os.path.expanduser('~'),
        'yellow_taxi',
        f'yellow_tripdata_{year_month}.csv'
    )

    with open(fpath, 'w', encoding='utf-8') as f:
        for chunk in response.iter_lines():
            f.write('{}\n'.format(chunk.decode('utf-8')))

    return fpath


def convert_to_parquet(year_month: str, file_path: str):
    df = pd.read_csv(file_path)

    current_month = dt.datetime.strptime(year_month, '%Y-%m')
    next_month = current_month.replace(month=current_month.month + 1)

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df = df[
        (df['tpep_pickup_datetime'] >= f'{current_month:%Y-%m-%d}') &
        (df['tpep_pickup_datetime'] < f'{next_month:%Y-%m-%d}')
        ]
    df['pickup_date'] = df['tpep_pickup_datetime'].dt.strftime('%Y-%m-%d')
    fpath = os.path.join(
        os.path.expanduser('~'),
        'yellow_taxi',
        f'yellow_tripdata_{year_month}.parquet'
    )
    df.to_parquet(fpath)
    return fpath
