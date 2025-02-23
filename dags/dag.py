from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator

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


def author_books_data(ti):
    author_data = ti.xcom_pull(key='book_list', task_ids='data-cleaning')

    final_data = create_author_books_df_with_count(author_data)
    final_data.xcom_push(key='author_list', value=final_data)


def year_conversion(ti):
    year_data = ti.xcom_pull(key='book_list', task_ids='data-cleaning')
    year_data['Year-Of-Publication'] = pd.to_numeric(year_data['Year-Of-Publication'], errors='coerce')
    year_data.xcom_push(key='year_data', value=year_data)




def insert_author_data_into_postgres(ti):

    author_data = ti.xcom_pull(key='author_list', task_ids='author_books_data_creation')


    if author_data.empty:
        raise ValueError("No book data found")

    postgres_hook = PostgresHook(postgres_conn_id='books_connection')
    insert_query = """
    INSERT INTO author (Book_Author, Book_Title, Book_Count)
    VALUES (%s, %s, %s)
    """
    for index, author in author_data.iterrows():
        # Ensure that author is a pandas Series, and access columns with string indices
        postgres_hook.run(insert_query, parameters=(
            author['Book_Author'],
            author['Book_Title'],
            author['Book_Count']
        ))


def insert_year_data_into_postgres(ti):

    year_data = ti.xcom_pull(key='author_list', task_ids='author_books_data_creation')


    if year_data.empty:
        raise ValueError("No book data found")

    postgres_hook = PostgresHook(postgres_conn_id='books_connection')
    insert_query = """
    INSERT INTO author (Book_Author, Book_Title, Book_Count)
    VALUES (%s, %s, %s)
    """
    for index, year in year_data.iterrows():
        postgres_hook.run(insert_query, parameters=(
            year['ISBN'],
            year['Book_Title'],
            year['Book_Author'],
            year['Year_Of_Publication'],
            year['Publisher']
        ))


# Dummy tasks can be replaced with slack notification


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

dag_start = DummyOperator(
    task_id='dag_start',
    dag=dag)


data_cleaning = PythonOperator(
    task_id='data_cleaning',
    python_callable=data_cleaning,
    op_kwargs={'data': data},
    provide_context=True,
    dag=dag
)


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
data_cleaning_end = DummyOperator(
    task_id='data_cleaning_end',
    dag=dag)

author_books_data_creation = DummyOperator(
    task_id='author_books_data_creation_start',
    dag=dag)

author_books_data_creation_start = PythonOperator(
    task_id='author_books_data_creation_start',
    python_callable=author_books_data,
    dag=dag,
)

create_author_table_task = PostgresOperator(
    task_id='create_author_table',
    postgres_conn_id='books_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS author (
        Book_Author TEXT,
        Book_Title TEXT,
        Book_Count bigint
    );
    """,
    dag=dag,
)

insert_author_data_task = PythonOperator(
    task_id='insert_author_data',
    python_callable=insert_author_data_into_postgres,
    dag=dag,
)

author_books_data_creation_end = DummyOperator(
    task_id='author_books_data_creation_end',
    dag=dag)

create_date_table_task = PostgresOperator(
    task_id='create_date_table_task',
    postgres_conn_id='books_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS final_books (
        ISBN TEXT PRIMARY KEY,
        Book_Title TEXT,
        Book_Author TEXT,
        Year_Of_Publication INT,
        Publisher TEXT 
    );
    """,
    dag=dag,
)


year_conversion_task = PythonOperator(
    task_id='year_conversion_task',
    python_callable=year_conversion,
    dag=dag,
)

insert_year_data_task = PythonOperator(
    task_id='insert_year_data',
    python_callable=insert_year_data_into_postgres,
    dag=dag,
)
""""
book_age = PythonOperator(
    task_id='Age_of_book',
    python_callable=calculate_book_age,
    provide_context=True,
    dag=dag,
)
"""

dag_end = DummyOperator(
    task_id='dag_end',
    dag=dag)

data_cleaning >> create_table_task >> insert_book_data_task

insert_book_data_task >> author_books_data_creation >> create_author_table_task >> insert_author_data_task >> dag_end

insert_book_data_task >> create_date_table_task >> year_conversion_task >> insert_year_data_task >> dag_end