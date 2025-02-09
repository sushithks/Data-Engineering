import requests
from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import time

from main import data_cleaning, create_author_books_df_with_count, calculate_book_age, year_conversion

df = pd.read_csv('C:/Users/sushi/OneDrive/Desktop/Sushith/DE/books_data/books.csv', delimiter=';', on_bad_lines='skip')


def insert_book_data_into_postgres(ti):
    book_data = ti.xcom_pull(key='book_list', task_ids='data-cleaning')
    if not book_data:
        raise ValueError("No book data found")

    postgres_hook = PostgresHook(postgres_conn_id='books_connection')
    insert_query = """
    INSERT INTO books (ISBN, Book-Title, Book-Author, Year-Of-Publication, Publisher)
    VALUES (%s, %s, %s, %s, %s)
    """
    for book in book_data:
        postgres_hook.run(insert_query, parameters=(book['ISBN'], book['Book-Title'], book['Book-Author'], book['Year-Of-Publication'], book['Publisher']))





default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'data_transfer',
    default_args=default_args,
    description='Load the local data in to Postgres',
    schedule_interval=timedelta(days=1)
)

data_cleaning = PythonOperator(
    task_id='data-cleaning',
    python_callable=data_cleaning,
    op_args=[df],
    provide_context=True,
    dag=dag
)
""""
author_table_creation = PythonOperator(
    task_id='data-cleaning',
    python_callable=create_author_books_df_with_count,
    op_args=df,
    dag=dag,
)
"""
year_conversion = PythonOperator(
    task_id='year_conversion',
    python_callable=year_conversion,
    provide_context=True,
    dag=dag,
)
book_age = PythonOperator(
    task_id='Age_of_book',
    python_callable=calculate_book_age,
    provide_context=True,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='books_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS books (
        ISBN VARCHAR(13) PRIMARY KEY,
        Book-Title TEXT,
        Book-Author TEXT,
        Year-Of-Publication TEXT,
        Publisher TEXT 
    );
    """,
    dag=dag,
)

insert_book_data_task = PythonOperator(
    task_id='insert_book_data',
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)

data_cleaning >> create_table_task >> insert_book_data_task



