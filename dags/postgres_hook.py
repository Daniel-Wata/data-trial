from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
import pandas as pd 

class CustomPostgresHook(BaseHook):
    """
    Custom hook to connect to a PostgreSQL database using SQLAlchemy.
    This hook uses the Airflow connection for the database credentials.
    """

    def __init__(self, conn_id):
        self.conn_id = conn_id

    def get_conn(self):
        """
        Retrieves the connection object for the PostgreSQL database.
        Returns an SQLAlchemy engine.
        """
        # Get the connection details from the Airflow connection
        conn = self.get_connection(self.conn_id)
        
        # Build the SQLAlchemy connection string
        connection_string = f'postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}'
        
        # Create the SQLAlchemy engine
        engine = create_engine(connection_string)
        
        return engine

    def get_pandas_df(self, query):
        """
        Executes a query and returns the result as a pandas DataFrame.
        """
        engine = self.get_conn()
        with engine.connect() as connection:
            return pd.read_sql(query, connection)