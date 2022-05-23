from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator


default_args = {
    'start_date': datetime(2020, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=15)
}
with DAG('bestseller_transactions', tags=['bestseller', 'products', 'reviews'], default_args=default_args,
         schedule_interval='00 02,10,18 * * *', catchup=False) as dag:
    process_bestseller_transactions = DockerOperator(
        task_id='process_bestseller_transactions',
        image='bestseller_web',
        api_version='auto',
        auto_remove=True,
        command=["python", "/project/app/main.py"],
        docker_url="tcp://docker-proxy:2375",
        network_mode="bridge"
    )

    process_bestseller_transactions
