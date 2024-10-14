from sentiment_analysis_functions import get_sentiment_textblob,get_sentiment_polyglot
from metrics_calculator import bayesian_average
from postgres_hook import CustomPostgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import numpy as np

# Function to clean data
def calculate_sentiment(**kwargs):
    ti = kwargs['ti']
    
    # Pull DataFrames from XCom
    df = ti.xcom_pull(key='df_reviews', task_ids='run_reviews_query')

    #Had some issues with docker with polyglot so, for the time I had, decided to go with textblob
    #df['sentiment_polyglot'] = df['review_text'].apply(get_sentiment_polyglot)
    df['sentiment_textblob'] = df['review_text'].apply(get_sentiment_textblob)

    #Looking at some samples I roughly saw that polyglot was doing better but didn't yield results in some rows
    #df['sentiment_score'] = np.where(df['sentiment_polyglot'].isna(),df['sentiment_textblob'],df['sentiment_polyglot'])
    df['sentiment_score'] = df['sentiment_textblob']

    # Push cleaned DataFrames to XCom
    kwargs['ti'].xcom_push(key='df_calculated_sentiment', value=df)

def group_sentiment_by_company(**kwargs):
    ti = kwargs['ti']
    
    # Pull DataFrames from XCom
    df = ti.xcom_pull(key='df_calculated_sentiment', task_ids='calculate_sentiment')

    df = df.groupby('google_id').agg(
                                                avg_sentiment_score=('sentiment_score', 'mean'),  
                                                num_evaluations=('sentiment_score', 'count') 
                                            ).reset_index() 
    
    # Global average sentiment score across all companies
    global_mean_rating = df['avg_sentiment_score'].mean()

    C = df['num_evaluations'].mean()  # Average number of reviews
    m = global_mean_rating     # Global average rating
    df['sentiment_average'] = df.apply(lambda row: bayesian_average(row['avg_sentiment_score'], row['num_evaluations'], C, m), axis=1)

    # Push cleaned DataFrames to XCom
    kwargs['ti'].xcom_push(key='df_grouped_sentiment', value=df)

# Function to write results to PostgreSQL using the custom hook
def write_results_to_db(**kwargs):
    ti = kwargs['ti']
    
    # Pull merged DataFrame from XCom
    merged_df = ti.xcom_pull(key='df_grouped_sentiment', task_ids='group_sentiment_by_company')

    # Use the custom hook to get the connection
    hook = CustomPostgresHook(conn_id='postgres_conn')
    engine = hook.get_conn()

    # Write the DataFrame directly to the table (full refresh)
    merged_df.to_sql(
        name='google_companies_sentiment',
        con=engine,
        if_exists='replace',  # Full refresh by replacing the table
        index=False
    )