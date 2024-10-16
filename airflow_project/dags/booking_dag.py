
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
import pandas as pd
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Define the default arguments
default_args = {
    'owner': 'cheep',
    'email': ['21120576@student.hcmus.edu.vn'],
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': days_ago(0),  # Set start_date to the current date and time
}

# Define the DAG
with DAG(
    dag_id='booking_scraping_dag',
    default_args=default_args,
    schedule_interval='0 8 * * *',
    catchup=False,
) as dag:

    # define the first task
    task1 = BashOperator(
        task_id='run_booking_scrapy',
        # cd /Users/mac/HCMUS/ItelligentAnalApp/python_scripts/airflow_temp/booking && scrapy crawl booking
        bash_command='cd /opt/airflow/booking && scrapy crawl accommodation',
        dag=dag,
    )

    task2 = BashOperator(
        task_id='run_price_scrapy',
        # cd /Users/mac/HCMUS/ItelligentAnalApp/python_scripts/airflow_temp/booking && scrapy crawl booking
        bash_command='cd /opt/airflow/booking && scrapy crawl price',
        dag=dag,
    )

    def fetch_data():
        raw_data = pd.read_csv('/opt/airflow/booking/hotel_data/2024-10-16-AccommodationItem.csv')
        return raw_data

    def transform_data(**kwargs):
        ti = kwargs['ti']
        # raw_df = ti.xcom_pull(task_ids='fetch_data')
        raw_df = fetch_data()
        # Thực hiện các bước transform 

        # %%
        def clean_description_text(series):
            return series.str.replace(r'[^\w\s]', '', regex=True).str.strip()

        # %%
        def transform_unities_column(unities_series):
            unities_clean = unities_series.str.replace(r'[\[\]]', '', regex=True).str.strip()
            unities_list = unities_clean.str.split(',').apply(lambda x: list(set(item.strip() for item in x if pd.notnull(item))))
            unities_dummies = unities_list.str.join('|').str.get_dummies()
            
            return pd.concat([unities_series.to_frame(), unities_dummies], axis=1)

        # %%
        def split_checkin_checkout_times(checkin_series, checkout_series):
            checkin_times = checkin_series.str.extract(r'From (\d{2}:\d{2}) to (\d{2}:\d{2})')
            checkin_times.columns = ['checkin_start', 'checkin_end']

            checkout_times = checkout_series.str.extract(r'From (\d{2}:\d{2}) to (\d{2}:\d{2})')
            checkout_times.columns = ['checkout_start', 'checkout_end']
            
            return checkin_times, checkout_times

        # %%
        def identify_pet_friendly(pet_info_series):
            return ~pet_info_series.str.contains('not allowed', case=False, na=False)

        # %%
        def extract_payment_methods_list(payment_methods_series):
            payment_methods_clean = payment_methods_series.str.replace(r'[\[\]]', '', regex=True).str.strip()
            payment_methods_list = payment_methods_clean.str.split(',').apply(lambda x: list(set(item.strip() for item in x if pd.notnull(item))))
            return payment_methods_list

        # %%
        df_test = pd.DataFrame()

        # %%
        df_test['description_clean'] = clean_description_text(raw_df['description'])
        df_test = transform_unities_column(raw_df['unities'])
        df_test[['checkin_start', 'checkin_end']], df_test[['checkout_start', 'checkout_end']] = split_checkin_checkout_times(raw_df['checkin'], raw_df['checkout'])
        df_test['pet_friendly'] = identify_pet_friendly(raw_df['petInfo'])
        df_test['payment_methods_list'] = extract_payment_methods_list(raw_df['paymentMethods'])


        # %%


        # %%
        df_pet = pd.DataFrame()
        df_unity = pd.DataFrame()

        # %%
        df_unity['unities_clean'] = raw_df['unities'].str.replace(r'[\[\]]', '', regex=True).str.strip()
        df_unity['unities_list'] = df_unity['unities_clean'].str.split(',').apply(lambda x: list(set(item.strip() for item in x)))
        unities_dummies = df_unity['unities_list'].str.join('|').str.get_dummies()
        df_unity = pd.concat([df_unity, unities_dummies], axis=1)
        df_unity.head()

        # %%
        df_pet['petInfo'] = raw_df['petInfo']
        df_pet['pet_allowed'] = raw_df['petInfo'].apply(lambda x: 0 if 'not allowed' in str(x).lower() else 1)
        df_pet.to_csv('/opt/airflow/booking/result.csv')
        
        # accomodation_df = data[['id', 'name', 'typeId', 'star', 'description', 'reviewScore', 'address', 'unities_clean', 'lat', 'lng']]
        # disciplines_df = data[['id', 'petInfo', 'paymentMethods', 'checkin', 'checkout']]
        
        # Push kết quả lại Airflow XCom
        # ti.xcom_push(key='accomodation_data', value=accomodation_df)
        # ti.xcom_push(key='disciplines_data', value=disciplines_df)
    
    task3 = PythonOperator(
            task_id='fetch_data',
            python_callable=fetch_data
        )

    task4 = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )
    
    task5 = PostgresOperator(
        task_id = 'create_postgres_table',
        postgres_conn_id = 'postgres_localhost',
        sql = """
            create table if not exists dag_runs123testt(
                dt date,
                dag_id character varying,
                primary key (dt,dag_id)
                )
        """     
    )


    task5
    # task pipeline
    # [task1, task2] >> fetch_data_task >> transform_data_task
