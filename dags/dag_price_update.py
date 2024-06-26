from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'price_user',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 25),
    'retry_delay': timedelta(minutes = 1)
}

dag = DAG('get_prices', default_args = default_args, schedule_interval='0 5 * * *',
          catchup=True, max_active_tasks=3, max_active_runs=1, tags=['price_update'])

extract_data_and_load = BashOperator(
    task_id='extract_data_and_load',
    bash_command='python3 /price_update_airflow/scripts/dag_price_update/for_update.py',
    dag = dag
)

