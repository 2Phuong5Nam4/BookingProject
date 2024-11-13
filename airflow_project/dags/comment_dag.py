
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
import pandas as pd
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import json
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from processData import *
from postgreQuery import *

CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")

# Define the default arguments
default_args = {
    'owner': 'cheep',
    'depends_on_past': False,
    'start_date': days_ago(0),  # Set start_date to the current date and time
}


class PushJsonToXComOperator(BaseOperator):
    @apply_defaults
    def __init__(self, file_path, xcom_key, chunk_size=100000, *args, **kwargs):
        super(PushJsonToXComOperator, self).__init__(*args, **kwargs)
        self.file_path = file_path
        self.xcom_key = xcom_key
        self.chunk_size = chunk_size

    def execute(self, context):
        # Read the JSON data from the file
        json_data = []
        with open(self.file_path, 'r', encoding='utf-8') as file:
            # read json line file
            json_data = [json.loads(line) for line in file]
        # Chunk the data
        chunks = [json_data[i:i + self.chunk_size] for i in range(0, len(json_data), self.chunk_size)]

        # Push the number of chunks to XCom
        self.xcom_push(context, key=f"{self.xcom_key}_num_chunks", value=len(chunks))
        # Push each chunk to XCom
        for i, chunk in enumerate(chunks):
            chunk_key = f"{self.xcom_key}_chunk_{i}"
            self.xcom_push(context, key=chunk_key, value=chunk)


        

def save_hotel_info_to_file(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='get_hotel_info_from_postgres')
    with open(f'/opt/airflow/booking/hotel_data/hotel_info.json', 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

def CommentProcess(**kwargs):
    ti = kwargs['ti']
    num_chunks = ti.xcom_pull(task_ids='push_json_comment_to_xcom', key='comment_json_data_num_chunks')
    print(f"Num chunks: {num_chunks}")
    
    # Retrieve each chunk
    json_data = []
    for i in range(num_chunks):
        chunk_key = f"comment_json_data_chunk_{i}"
        chunk = ti.xcom_pull(task_ids="push_json_comment_to_xcom", key=chunk_key)
        df = pd.DataFrame(chunk)

        df.to_csv(f'/opt/airflow/logs/chunk_{i}.csv', index=False)
        print(f'Successfully! {df}')
        json_data.extend(chunk)
    print(f'==> {json_data}')
    # Convert the combined JSON data into a Pandas DataFrame
    
        

# Define the DAG
with DAG(
    dag_id='comment_scraping_dag',
    default_args=default_args,
    schedule_interval='0 17 * * *',
    catchup=False,
) as dag:

    # task request url from postgres database
    get_hotel_info_task = PythonOperator(
        task_id='get_hotel_info_from_postgres', 
        python_callable=extract_data_from_postgres,
        op_kwargs={'sql_query': 'SELECT acm_id, acm_location, acm_review_count from public."Accommodation"'},
        provide_context=True,  # This passes the execution context (including the execution date) to the callable
        dag=dag,
    )

    save_url_task = PythonOperator(
        task_id='save_hotel_info_to_file',
        python_callable=save_hotel_info_to_file,
        provide_context=True,  # This passes the execution context (including the execution date) to the callable
        dag=dag,
    )


    task2 = BashOperator(
        task_id='run_comment_spider',
        # cd /Users/mac/HCMUS/ItelligentAnalApp/python_scripts/airflow_temp/booking && scrapy crawl booking
        bash_command=f'cd /opt/airflow/booking && scrapy crawl comment -o /opt/airflow/booking/hotel_data/{CURRENT_DATE}-CommentItem.jl',
        dag=dag,
    )


    # push_json_comment_task = PushJsonToXComOperator(
            
    #     task_id='push_json_comment_to_xcom',
    #     file_path=f'/opt/airflow/booking/hotel_data/{CURRENT_DATE}-CommentItem.jl',  # Adjust the file path as needed
    #     xcom_key='comment_json_data',
    #     dag=dag,
    # )
    
    # process_comment_task = PythonOperator(
    #     task_id='process_comment_data',
    #     python_callable=CommentProcess,
    #     provide_context=True,  
    #     op_kwargs={'execution_date': '{{ ds }}'} 
    # )


    # task5
    # task pipeline
    get_hotel_info_task >> save_url_task >> task2 
    # push_json_comment_task >> process_comment_task
    # push_json_price_task >> process_room_task >> process_bed_price_task
