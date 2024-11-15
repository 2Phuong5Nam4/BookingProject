from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
import pandas as pd
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import json
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from processData import *
from postgreQuery import *
from airflow.utils.task_group import TaskGroup
import os

# Remove the CURRENT_DATE definition as we'll use specific dates

# Define the default arguments
default_args = {
    'owner': 'cheep',
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
        if os.path.exists(self.file_path):
            with open(self.file_path, 'r', encoding='utf-8') as file:
                # read json line file
                json_data = [json.loads(line) for line in file]

            # Push the JSON data to XCom
            self.xcom_push(context, key=self.xcom_key, value=json_data)
        else:
            self.log.info('File not found')

def save_url_to_file(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='get_url_from_postgres')
    with open(f'/opt/airflow/booking/hotel_data/url.json', 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)


# Create a list of dates to process

dates_to_process = ['2024-10-31', '2024-11-01', '2024-11-02', '2024-11-03', '2024-11-04', '2024-11-05', '2024-11-06', '2024-11-07', '2024-11-08']

with DAG(
    dag_id ='price_scraping_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    previous_date_group = None
    
    for process_date in dates_to_process:
        with TaskGroup(group_id=f'process_date_{process_date}') as date_group:
            push_json_price_task = PushJsonToXComOperator(
                task_id='push_json_price_to_xcom',
                file_path=f'/opt/airflow/booking/hotel_data/price_data/{process_date}-RoomPriceItem.jl',
                xcom_key=f'scrapy_json_data',
            )

            process_room_task = PythonOperator(
                task_id='process_room_data',
                python_callable=RoomProcess,
                provide_context=True,
                op_kwargs={'execution_date': process_date},
            )

            process_bed_price_task = PythonOperator(
                task_id='process_bed_price_data',
                python_callable=BedPriceProcess,
                provide_context=True,
                op_kwargs={'execution_date': process_date},
            )

            # Set up task dependencies within the group
            push_json_price_task >> process_room_task >> process_bed_price_task

        # Link this date group to the previous one for sequential processing
        if previous_date_group:
            previous_date_group >> date_group
        
        previous_date_group = date_group
