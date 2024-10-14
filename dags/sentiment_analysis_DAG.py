from datetime import datetime, timedelta
from scripts.sentiment_analysis_pipeline import *
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import postgres_functions
from postgres_hook import CustomPostgresHook

"""
DAG to extract company data from Google Maps, calculate the bayesian average based on customer reviews, 
and write the results back to the database.

DAG Details:
- Extracts company profiles and customer review data from PostgreSQL.
- Calculates the bayesian average based on review ratings and counts.
- Sorts the results and writes them back to the database.

"""


default_args = {
    "owner": "daniel.wata",
    "start_date": datetime(2024, 10, 1),
}


with DAG("sentiment_analysis", default_args=default_args, catchup=False, schedule_interval='@daily', max_active_runs=1) as dag:

    fetch_data = PythonOperator(
        task_id='run_reviews_query',
        python_callable=postgres_functions.run_query,
        op_kwargs={
            'query': """SELECT google_id,review_id,review_text FROM customer_reviews_google where review_text is not null and review_text != ''""",
            'key': 'df_reviews'
        },
        provide_context=True
    )
    
    calculate_sentiment_task = PythonOperator(
        task_id='calculate_sentiment',
        python_callable=calculate_sentiment,
        provide_context=True,
        execution_timeout=timedelta(minutes=1)
    )

    group_sentiment_by_company_task = PythonOperator(
        task_id='group_sentiment_by_company',
        python_callable=group_sentiment_by_company,
        provide_context=True
    )
    
    
    write_results = PythonOperator(
        task_id='write_results_to_db',
        python_callable=write_results_to_db,
        provide_context=True
    )
    
    # Task dependencies
    fetch_data >> calculate_sentiment_task >> group_sentiment_by_company_task >> write_results
