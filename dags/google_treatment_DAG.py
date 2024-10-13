from datetime import datetime, timedelta
from scripts.google_treatment_pipeline import *
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


with DAG("google_treatment", default_args=default_args, catchup=False, schedule_interval='@daily', max_active_runs=1) as dag:

    fetch_data = PythonOperator(
        task_id='run_companies_query',
        python_callable=postgres_functions.run_query,
        op_kwargs={
            'query': """SELECT  c.google_id, 
                                c.name, 
                                c.type, 
                                c.category,
                                c.us_state,
                                c.city,
                                cr.rating,
                                cr.reviews as number_of_reviews
                        FROM company_profiles_google_maps c 
                        INNER JOIN (SELECT google_id, MAX(reviews) as reviews, AVG(rating) as rating FROM customer_reviews_google GROUP BY google_id) cr on cr.google_id = c.google_id""",
            'key': 'df_companies'
        },
        provide_context=True
    )
    
    calculate_metric_task = PythonOperator(
        task_id='calculate_metric',
        python_callable=calculate_metric,
        provide_context=True
    )
    
    sort_values = PythonOperator(
        task_id='sort_dataframe',
        python_callable=sort_data,
        provide_context=True
    )
    
    write_results = PythonOperator(
        task_id='write_results_to_db',
        python_callable=write_results_to_db,
        provide_context=True
    )
    
    # Task dependencies
    fetch_data >> calculate_metric_task >> sort_values >> write_results
