import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class CurrencyScoopHook(BaseHook):

    def __init__(self, currency_conn_id: str):
        super().__init__()
        self.conn_id = currency_conn_id

    def get_rate(self, date, base_currency: str, symbol: str):
        url = 'https://api.currencyscoop.com/v1/historical'
        params = {
            'base': base_currency.upper(),
            'symbols': symbol.upper(),
            'api_key': self._get_api_key(),
            'date': str(date),
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()['response']['rates'][symbol]

    def _get_api_key(self):
        conn = self.get_connection(self.conn_id)
        if not conn.password:
            raise AirflowException('Missing API key (password) in connection settings')
        return conn.password
