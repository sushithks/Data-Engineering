from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'amazon_books_data_transfer',
    default_args=default_args,
    description='Fetch data from Amazon and store it in Postgres',
    schedule_interval=timedelta(days=1),
)