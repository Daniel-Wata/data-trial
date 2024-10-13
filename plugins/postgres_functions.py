from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

# Reusable function to run SQL queries and push result to XCom
def run_query(query, key, **kwargs):
    # Use PostgresHook to fetch the connection
    hook = PostgresHook(postgres_conn_id='postgres_conn')
    connection = hook.get_conn()

    # Execute the query and fetch the result as a DataFrame
    df = pd.read_sql(query, connection)

    # Push the DataFrame to XCom with a specified key
    kwargs['ti'].xcom_push(key=key, value=df)