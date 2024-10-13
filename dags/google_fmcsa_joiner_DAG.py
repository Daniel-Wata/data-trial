from datetime import datetime, timedelta
from scripts.google_fmcsa_joiner_pipeline import *
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

"""
DAG to integrate data from Google and FMCSA, perform fuzzy matching, and store the results back to the database.

DAG Details:
- Fetches data from Google and FMCSA databases.
- Cleans the data to prepare it for matching.
- Performs fuzzy matching to integrate similar records from the two datasets.
- Writes the results back to the database for future analysis.


"""




default_args = {
    "owner": "daniel.wata",
    "start_date": datetime(2024, 10, 1),
}


with DAG("google_fmcsa_joiner_DAG", default_args=default_args, catchup=False, schedule_interval='@daily', max_active_runs=1) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_data_from_db',
        python_callable=fetch_data_from_db,
        provide_context=True
    )
    
    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
        provide_context=True
    )
    
    fuzzy_match = PythonOperator(
        task_id='perform_fuzzy_matching',
        python_callable=perform_fuzzy_matching,
        provide_context=True
    )
    
    write_results = PythonOperator(
        task_id='write_results_to_db',
        python_callable=write_results_to_db,
        provide_context=True
    )
    
    # Task dependencies
    fetch_data >> clean_data_task >> fuzzy_match >> write_results
