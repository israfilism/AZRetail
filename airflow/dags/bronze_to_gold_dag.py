from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'azretail_bronze_to_gold',
    default_args=default_args,
    schedule_interval = None,
    catchup=False,
) as dag:

    api_currency = BashOperator(
        task_id='api_currency',
        bash_command='python /opt/airflow/dags/scripts/api_currency_to_db.py',
    )

    run_silver_models = BashOperator(
        task_id='dbt_run_silver',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select silver_customers silver_products silver_orders',
    )

    run_snapshots = BashOperator(
        task_id='dbt_snapshot',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt snapshot',
    )

    run_gold_models = BashOperator(
        task_id='dbt_run_gold',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select gold_dim_customer gold_dim_product gold_fct_order_items',
    )

    run_tests = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test',
    )

    api_currency >> run_silver_models >> run_snapshots >> run_gold_models >> run_tests