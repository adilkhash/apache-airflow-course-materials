import os

from fabric import Connection, task

ssh_key_path = os.path.join(os.path.expanduser('~'), '.ssh', 'id_rsa')
HOSTNAME = 'flow.dataeng.ru'
USERNAME = 'airflow'


@task
def deploy(context):
    with Connection(
        HOSTNAME,
        connect_kwargs={'key_filename': ssh_key_path},
        user=USERNAME,
    ) as conn:
        with conn.cd('~/repositories/dataeng_dags'):
            with conn.prefix('source ~/venvs/.airflow/bin/activate'):
                _ = conn.run('git pull')
                print(f'{_.stdout.strip()}')
                _ = conn.run('pip install -r requirements.txt', hide=True)
                print(f'{_.stdout.strip()}')
                _ = conn.run('cp -R dags/* ~/airflow/dags/', hide=True)
                print(f'{_.stdout.strip()}')
