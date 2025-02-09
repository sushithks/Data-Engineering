import requests
from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import time

from main import data_creation, data_cleaning, create_author_books_df_with_count, calculate_book_age, year_conversion

data = data_creation()


def insert_book_data_into_postgres(ti):

    json_book_data = ti.xcom_pull(key='book_list', task_ids='data-cleaning')
    book_data = pd.read_json(json_book_data)

    if book_data.empty:
        raise ValueError("No book data found")

    postgres_hook = PostgresHook(postgres_conn_id='books_connection')
    insert_query = """
    INSERT INTO books (ISBN, Book_Title, Book_Author, Year_Of_Publication, Publisher)
    VALUES (%s, %s, %s, %s, %s)
    """
    for index, book in book_data.iterrows():
        # Ensure that book is a pandas Series, and access columns with string indices
        postgres_hook.run(insert_query, parameters=(
            book['ISBN'],
            book['Book_Title'],
            book['Book_Author'],
            book['Year_Of_Publication'],
            book['Publisher']
        ))



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
    op_kwargs={'data': data},
    provide_context=True,
    dag=dag
)
"""
author_table_creation = PythonOperator(
    task_id='data-cleaning',
    python_callable=create_author_books_df_with_count,
    op_args=df,
    dag=dag,
)

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
"""
create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='books_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS books (
        ISBN TEXT PRIMARY KEY,
        Book_Title TEXT,
        Book_Author TEXT,
        Year_Of_Publication TEXT,
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



