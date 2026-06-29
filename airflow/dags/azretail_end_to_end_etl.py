from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import sys

sys.path.append('/opt/airflow/dags/scripts')
from ingest_csv_to_bronze import ingest_csv
from ingest_json_to_bronze import ingest_json

config_list = Variable.get("raw_etl_tables", deserialize_json=True)
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'end_date': datetime(2026, 1, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'concurrency': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'azretail_end_to_end_etl', 
    default_args=default_args,
    schedule_interval='@daily',
    max_active_runs=1,
    catchup=True,
) as dag:

    raw_tasks = []
    
    for params in config_list:
        task_id_name = params['target_table'].replace('raw_', '')
        if params['file_type'] == 'json':
            callable_func = ingest_json
        elif params['file_type'] == 'csv':
            callable_func = ingest_csv
            
        op_kwargs_params = {
            'file_name': params['file_name'],
            'target_table': params['target_table'],
            'ds': '{{ ds }}'
        }
        
        task = PythonOperator(
            task_id=f'from_raw_to_{task_id_name}',
            python_callable=callable_func,
            op_kwargs=op_kwargs_params
        )
        raw_tasks.append(task)

    api_currency = BashOperator(
        task_id='api_currency',
        bash_command='python /opt/airflow/dags/scripts/api_currency_to_db.py',
    )

    run_silver_models = BashOperator(
        task_id='dbt_run_silver',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select silver_customers silver_products silver_orders --vars "execution_date: {{{{ ds }}}}"',
    )

    run_snapshots = BashOperator(
        task_id='dbt_snapshot',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt snapshot',
    )

    run_gold_models = BashOperator(
        task_id='dbt_run_gold',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select gold_dim_customer gold_dim_product gold_fct_order_items --vars "execution_date: {{{{ ds }}}}"',
    )

    run_tests = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test',
    )


    raw_tasks >> api_currency
    
    api_currency >> run_silver_models >> run_snapshots >> run_gold_models >> run_tests