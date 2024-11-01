from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime


def extract_data_from_postgres():
    # Create a Postgres hook (ensure the connection ID matches your Airflow connection)
    pg_hook = PostgresHook(postgres_conn_id='my_rds_postgres_conn')

    # Define your SQL query
    sql_query = 'SELECT acm_id, acm_url from public."Accommodation"'

    # Execute the query and fetch results
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql_query)
    results = cursor.fetchall()  # This fetches all rows from the query

    # Convert the results to a list of dictionaries
    results = [{"id": row[0], "url": row[1]} for row in results]
    return results
