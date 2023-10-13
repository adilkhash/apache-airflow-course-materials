FROM apache/airflow:2.7.2-python3.11
COPY requirements.txt /tmp
RUN pip install --no-cache-dir -r /tmp/requirements.txt
