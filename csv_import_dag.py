import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

DAG_START_DATE = datetime(2023, 1, 14, 14, 10)

DAG_DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': DAG_START_DATE,
    'retries': 1,
    'retry_delay': timedelta(seconds = 5)
}

def read_csv(filepath):
    df = pd.read_csv(filepath)
    # Perform operations on DataFrame
    return df

with DAG('pandas_csv_dag', default_args= DAG_DEFAULT_ARGS, schedule_interval='0 1 * * *') as dag:

    read_csv_task = PythonOperator(
        task_id='read_csv',
        python_callable=read_csv,
        op_kwargs={'filepath': '/path/to/your/file.csv'},
        dag=dag
        )
    read_csv_task