from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from postgres_hook import CustomPostgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from metrics_calculator import bayesian_average

# Function to clean data
def calculate_metric(**kwargs):
    ti = kwargs['ti']
    
    # Pull DataFrames from XCom
    df = ti.xcom_pull(key='df_companies', task_ids='run_companies_query')

    # Global average rating across all companies (used in Bayesian calculation)
    global_mean_rating = df['rating'].mean()

    C = df['number_of_reviews'].mean()  # Average number of reviews
    m = global_mean_rating     # Global average rating
    
    df['bayesian_average'] = df.apply(lambda row: bayesian_average(row['rating'], row['number_of_reviews'], C, m), axis=1)
    # Push cleaned DataFrames to XCom
    kwargs['ti'].xcom_push(key='df_calculated_metrics', value=df)

# Function to clean data
def sort_data(**kwargs):
    ti = kwargs['ti']
    
    # Pull DataFrames from XCom
    df = ti.xcom_pull(key='df_calculated_metrics', task_ids='calculate_metric')

    df = df.sort_values(by='bayesian_average', ascending = False)
    # Push cleaned DataFrames to XCom
    kwargs['ti'].xcom_push(key='df_sorted', value=df)


# Function to write results to PostgreSQL using the custom hook
def write_results_to_db(**kwargs):
    ti = kwargs['ti']
    
    # Pull merged DataFrame from XCom
    merged_df = ti.xcom_pull(key='df_sorted', task_ids='sort_dataframe')

    # Use the custom hook to get the connection
    hook = CustomPostgresHook(conn_id='postgres_conn')
    engine = hook.get_conn()

    # Write the DataFrame directly to the table (full refresh)
    merged_df.to_sql(
        name='google_companies_treated',
        con=engine,
        if_exists='replace',  # Full refresh by replacing the table
        index=False
    )