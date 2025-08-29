from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator

import sys
sys.path.append("/price_update_airflow")

bash_command="/home/ubuntu/airflow/venv/bin/python /home/ubuntu/airflow-project/price_update_airflow/main.py"


default_args = {
    'owner': 'price_user',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 12),
    'retry_delay': timedelta(minutes = 5)
}

dag = DAG('update_from_main',
            default_args = default_args,
            schedule_interval='@daily',
            catchup=False,
            max_active_tasks=3,
            max_active_runs=1,
            tags=['price_update'])

extract_data_and_load = BashOperator(
    task_id='extract_data_and_load',
    bash_command=bash_command,
    dag = dag
)
