from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import sys

sys.path.append('/opt/airflow/dags/scripts')
from ingest_csv_to_bronze import ingest_csv
from ingest_json_to_bronze import ingest_json

config_list = Variable.get("raw_etl_tables", deserialize_json=True)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'concurrency':1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'azretail_raw_to_bronze',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    

    for params in config_list:
        task_id_name = params['target_table'].replace('raw_', '')
        if params['file_type'] == 'json':
            callable_func = ingest_json
        elif params['file_type'] == 'csv':
            callable_func = ingest_csv
            
        op_kwargs_params = {
                'file_name': params['file_name'],
                'target_table': params['target_table']
            }
        PythonOperator(
            task_id=f'from_raw_to_{task_id_name}',
            python_callable=callable_func,
            op_kwargs=op_kwargs_params
        )
    
