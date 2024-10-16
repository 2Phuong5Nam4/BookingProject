from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime
import pandas as pd
import os

def fetch_data():
    data = pd.read_csv('2024-10-15-AccommodationItem.csv')
    return data

def transform_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_data')
    
    # Thực hiện các bước transform 
    #ABCXYZ
    
    accomodation_df = data[['id', 'name', 'typeId', 'star', 'description', 'reviewScore', 'address', 'unities_clean', 'lat', 'lng']]
    disciplines_df = data[['id', 'petInfo', 'paymentMethods', 'checkin', 'checkout']]
    
    # Push kết quả lại Airflow XCom
    ti.xcom_push(key='accomodation_data', value=accomodation_df)
    ti.xcom_push(key='disciplines_data', value=disciplines_df)

def load_data_to_db(**kwargs):
    ti = kwargs['ti']
    
    # Connect PostgreSQL DB
    #pg_hook = PostgresHook(postgres_conn_id='ABCXYZ')
    # Connect tới MSSQL
    mssql_hook = MsSqlHook(mssql_conn_id='ABCXYZ')
    accomodation_data = ti.xcom_pull(task_ids='transform_data', key='accomodation_data')
    disciplines_data = ti.xcom_pull(task_ids='transform_data', key='disciplines_data')
    
    #accomodation_data.to_sql('accomodation', pg_hook.get_sqlalchemy_engine(), if_exists='replace', index=False)
    #disciplines_data.to_sql('disciplines', pg_hook.get_sqlalchemy_engine(), if_exists='replace', index=False)

    # Chuyển đổi DataFrame sang SQL và tải vào database
    engine = mssql_hook.get_sqlalchemy_engine()
    accomodation_data.to_sql('accomodation', engine, if_exists='replace', index=False)
    disciplines_data.to_sql('disciplines', engine, if_exists='replace', index=False)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('data_transformation_pipeline', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    fetch_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data
    )

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )

    load_data_task = PythonOperator(
        task_id='load_data_to_db',
        python_callable=load_data_to_db,
        provide_context=True
    )

    fetch_data_task >> transform_data_task >> load_data_task
