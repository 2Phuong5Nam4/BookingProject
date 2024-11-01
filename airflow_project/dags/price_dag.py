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
    'email': ['21120576@student.hcmus.edu.vn'],
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': days_ago(0),  # Set start_date to the current date and time
}


class PushJsonToXComOperator(BaseOperator):
    @apply_defaults
    def __init__(self, file_path, xcom_key, *args, **kwargs):
        super(PushJsonToXComOperator, self).__init__(*args, **kwargs)
        self.file_path = file_path
        self.xcom_key = xcom_key

    def execute(self, context):
        # Read the JSON data from the file
        json_data = []
        with open(self.file_path, 'r', encoding='utf-8') as file:
            # read json line file
            json_data = [json.loads(line) for line in file]

        # Push the JSON data to XCom
        self.xcom_push(context, key=self.xcom_key, value=json_data)


def save_url_to_file(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='get_url_from_postgres')
    with open(f'/home/ubuntu/airflow/booking/hotel_data/url.json', 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)


# Define the DAG
with DAG(
        dag_id='price_scraping_dag',
        default_args=default_args,
        schedule_interval='0 8 * * *',
        catchup=False,
) as dag:
    # # task request url from postgres database
    # get_url_task = PythonOperator(
    #     task_id='get_url_from_postgres',
    #     python_callable=extract_data_from_postgres,
    #     provide_context=True,  # This passes the execution context (including the execution date) to the callable
    #     dag=dag,
    # )

    # save_url_task = PythonOperator(
    #     task_id='save_url_to_file',
    #     python_callable=save_url_to_file,
    #     provide_context=True,  # This passes the execution context (including the execution date) to the callable
    #     dag=dag,
    # )

    # task2 = BashOperator(
    #     task_id='run_price_scrapy',
    #     # cd /Users/mac/HCMUS/ItelligentAnalApp/python_scripts/airflow_temp/booking && scrapy crawl booking
    #     bash_command=f'cd /home/ubuntu/airflow/booking && scrapy crawl price -o /home/ubuntu/airflow/booking/hotel_data/{CURRENT_DATE}-RoomPriceItem.jl',
    #     dag=dag,
    # )

    push_json_price_task = PushJsonToXComOperator(

        task_id='push_json_price_to_xcom',
        file_path=f'/home/ubuntu/airflow/booking/hotel_data/{CURRENT_DATE}-RoomPriceItem.jl',
        # Adjust the file path as needed
        xcom_key='scrapy_json_data',
        dag=dag,
    )

    process_room_task = PythonOperator(
        task_id='process_room_data',
        python_callable=RoomProcess,
        provide_context=True,
        op_kwargs={'execution_date': '{{ ds }}'}
    )

    process_bed_price_task = PythonOperator(
        task_id='process_be_price_data',
        python_callable=BedPriceProcess,
        provide_context=True,
        op_kwargs={'execution_date': '{{ ds }}'}
    )

    # task5
    # task pipeline
    # get_url_task >> save_url_task >> task2 >> push_json_price_task >> process_room_task >> process_bed_price_task
    push_json_price_task >> process_room_task >> process_bed_price_task
