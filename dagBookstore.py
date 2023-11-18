# import required libraries
from airflow import DAG
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
from dagSubTasksBk import clean_dataframe, transform_dataframe, get_tables_sql, create_table_destination_sql
import pandas as pd
import json




default_args = {
    'owner': 'Mohamed Medhat',
    'start_date': datetime(2023, 10, 31)
}




def read_psql_to_df(ti):    # sub task1
    # using postgres_hook for connecting to postgresql and get data with sql
    postgres_hook = PostgresHook(postgres_conn_id='postgres_books')
    psql_data = postgres_hook.get_pandas_df(get_tables_sql)
    print(f'There are {len(psql_data)} rows has integrated from the PosgreSQL.')
    ti.xcom_push(key='df_psql', value=psql_data)


def read_json_to_df(ti):    # sub task2
    # read json file
    with open('/home/momedhat/airflow/dags/Books.json', 'r') as json_file:
        data = json.load(json_file)
        df_json = pd.DataFrame(data)
        print(f'There are {len(df_json)} rows has integrated from the JSON file.')
        ti.xcom_push(key='df_json', value=df_json)


def merge_df(ti):   # sub task3
    # combine the two dataFrames into one df
    df1 = ti.xcom_pull(task_ids='get_postgres_data_src', key='df_psql')
    df2 = ti.xcom_pull(task_ids='get_json_data_src', key='df_json')
    df2.drop(columns='id', inplace=True)

    appended_df = pd.merge(df1, df2, how='outer') 
    ti.xcom_push(key ='merged_df', value=appended_df)   # pass to task4 for cleaning

    print("The two data frames has merged successfully.")
    print(appended_df.head())


def clean_df(ti):   # sub task4 
    # pass the data frame to clean_dataframe method 
        # clean_dataframe method will check the nulls and duplicates
    df = ti.xcom_pull(task_ids='data_merging', key='merged_df')
    cleaned_df = clean_dataframe(df=df)

    print("Data has cleaned successfully.")
    ti.xcom_push(key='cleaned_df', value=cleaned_df)    # pass to task5 for transforming


def transform_df(ti):   # sub task5
    # get the cleaned dataFrame to filter out from useless data
    # pass the data frame to transform_dataframe method 
    df = ti.xcom_pull(task_ids='data_cleaning', key='cleaned_df')
    transformed_df = transform_dataframe(df=df)

    print("Data has transformed successfully.")
    ti.xcom_push(key='transformed_df', value=transformed_df)    # pass to task7 for inserting to DB
   

def save_data_csv(ti):  # sub task8
    # save the data into clean_data.csv file
    df = ti.xcom_pull(task_ids='data_transformation', key='transformed_df')
    df.to_csv('/home/momedhat/airflow/dags/clean_data.csv', index=False)
    print('Data has saved into clean_data.csv')

def insert_df_into_postgres(ti):    # sub task7
    # insert the clean DataFrame into PostgreSQL table

    df = ti.xcom_pull(task_ids='data_transformation', key='transformed_df')
    
    # Specify the connection parameters
    conn_id = "postgres_books"
    conn = BaseHook.get_connection(conn_id)
    conn_uri = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"

    # Create SQLAlchemy engine
    engine = create_engine(conn_uri)

    # Use df.to_sql() to insert data into PostgreSQL
    df.to_sql('books_destination', engine, if_exists='replace', index=False, chunksize=1000)

    print("Data has been inserted into the table successfully.")







with DAG (
    dag_id = "bookstore_exploring",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False    
)as dag :


    ########### EXTRACT ###########

    # task1 : import from postgresql
    get_data_psql = PythonOperator(
        task_id='get_postgres_data_src',
        python_callable = read_psql_to_df
    )

    # task 2 : import from JSON file 
    get_data_json = PythonOperator(
        task_id='get_json_data_src',
        python_callable = read_json_to_df
    )

    # task 3 : merge the two dataFrames into one
    merge_data = PythonOperator(
        task_id='data_merging',
        python_callable = merge_df
    )


    ########### TRANSFORM ###########

    # task 4 : Cleaning data - check duplicated and null values 
    clean_data = PythonOperator(
        task_id='data_cleaning',
        python_callable = clean_df
    )

    # task 5 : Transforming data - remove useless columns and change data types 
    transform_data = PythonOperator(
        task_id='data_transformation',
        python_callable = transform_df
    )
    
    
    ########### LOAD ###########
    
    # task 6 : Create table - create (books_destination) table into Books database that will store the transformed clean data  
    create_table_dest = PostgresOperator(
        task_id='create_postgres_table_destination',
        sql=create_table_destination_sql,
        postgres_conn_id='postgres_books'
    )

    # task 7 : Insert the clean data into the (books_destination) table  
    insert_data_into_table_dest = PythonOperator(
        task_id='insert_data_into_table',
        python_callable=insert_df_into_postgres
    )

    # task 8 : Save the clean data as CSV file  
    save_data = PythonOperator(
        task_id='save_data_csv',
        python_callable = save_data_csv
    )





    [get_data_psql, get_data_json] >> merge_data >> clean_data >> transform_data 
    transform_data >> create_table_dest >> insert_data_into_table_dest
    transform_data >> save_data