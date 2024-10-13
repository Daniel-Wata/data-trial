from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from postgres_hook import CustomPostgresHook
import re
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

def fetch_data_from_db(**kwargs):
    # Use PostgresHook to fetch the connection
    hook = PostgresHook(postgres_conn_id='postgres_conn')
    connection = hook.get_conn()

    df_fmcsa = pd.read_sql('SELECT usdot_num, company_name, registered_address, phone_number FROM public.fmcsa_company_snapshot', connection)
    df_google = pd.read_sql('SELECT google_id, name, full_address, phone FROM company_profiles_google_maps', connection)

    kwargs['ti'].xcom_push(key='df_fmcsa', value=df_fmcsa)
    kwargs['ti'].xcom_push(key='df_google', value=df_google)

# Function to clean data
def clean_data(**kwargs):
    ti = kwargs['ti']
    
    # Pull DataFrames from XCom
    df_fmcsa = ti.xcom_pull(key='df_fmcsa', task_ids='fetch_data_from_db')
    df_google = ti.xcom_pull(key='df_google', task_ids='fetch_data_from_db')
    
    # Cleaning functions
    def clean_phone_number(phone):
        if not phone:
            return ""
        phone = re.sub(r'^\+1\s*', '', phone)
        phone = re.sub(r'\D', '', phone)
        return phone

    #fill NAs
    df_fmcsa = df_fmcsa.fillna("")
    df_google = df_google.fillna(" ")

    # Apply cleaning
    df_fmcsa['treated_phone'] = df_fmcsa['phone_number'].apply(clean_phone_number)
    df_google['treated_phone'] = df_google['phone'].apply(clean_phone_number)
    
    # Push cleaned DataFrames to XCom
    kwargs['ti'].xcom_push(key='df_fmcsa_clean', value=df_fmcsa)
    kwargs['ti'].xcom_push(key='df_google_clean', value=df_google)

# Function to perform fuzzy matching
def perform_fuzzy_matching(**kwargs):
    """
        Perform fuzzy matching on cleaned data.

        This task applies fuzzy matching techniques to find similar records
        across the Google and FMCSA datasets. It helps merge the data
        based on approximate matches.
    """
    ti = kwargs['ti']
    
    # Pull cleaned DataFrames from XCom
    df_fmcsa = ti.xcom_pull(key='df_fmcsa_clean', task_ids='clean_data')
    df_google = ti.xcom_pull(key='df_google_clean', task_ids='clean_data')
    
    # Combine columns for fuzzy matching
    df_fmcsa['fuzzy_column'] = df_fmcsa['treated_phone'] + ' ' + df_fmcsa['registered_address'] + ' ' + df_fmcsa['company_name']
    df_google['fuzzy_column'] = df_google['treated_phone'] + ' ' + df_google['full_address'] + ' ' + df_google['name']
    
    df_google['fuzzy_column'] = df_google['fuzzy_column'].str.upper().replace(r'[\r\t]', ' ', regex=True)
    df_fmcsa['fuzzy_column'] = df_fmcsa['fuzzy_column'].str.upper().replace(r'[\r\t]', ' ', regex=True)
    
    # Fuzzy matching logic
    s = df_google['fuzzy_column'].tolist()
    matches = []
    for idx, row in df_fmcsa.iterrows():
        match = process.extractOne(row['fuzzy_column'], s, scorer=fuzz.token_sort_ratio)
        if match and match[1] >= 73:
            matched_row = df_google[df_google['fuzzy_column'] == match[0]].iloc[0]
            match_data = {
                'left_combined_info': row['fuzzy_column'],
                'right_combined_info': match[0],
                'match_score': match[1]
            }
            match_data.update(row.to_dict())
            match_data.update(matched_row.to_dict())
            matches.append(match_data)
    
    merged_df = pd.DataFrame(matches)

    merged_df_columns = merged_df[['google_id', 'usdot_num']]
    
    # Push merged DataFrame to XCom
    kwargs['ti'].xcom_push(key='merged_df', value=merged_df_columns)

# Function to write results to PostgreSQL using the custom hook
def write_results_to_db(**kwargs):
    ti = kwargs['ti']
    
    # Pull merged DataFrame from XCom
    merged_df = ti.xcom_pull(key='merged_df', task_ids='perform_fuzzy_matching')

    # Use the custom hook to get the connection
    hook = CustomPostgresHook(conn_id='postgres_conn')
    engine = hook.get_conn()

    # Write the DataFrame directly to the table (full refresh)
    merged_df.to_sql(
        name='google_fmcsa_comparison',
        con=engine,
        if_exists='replace',  # Full refresh by replacing the table
        index=False
    )