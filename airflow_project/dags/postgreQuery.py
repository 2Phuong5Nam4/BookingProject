from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime




def extract_data_from_postgres(sql_query):
    # Create a Postgres hook (ensure the connection ID matches your Airflow connection)
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    
    # Execute the query and fetch results
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql_query)
    results = cursor.fetchall()  # This fetches all rows from the query
    
    # Convert the results to a list of dictionaries
    columns = [col[0] for col in cursor.description]
    results = [dict(zip(columns, row)) for row in results]
    return results
