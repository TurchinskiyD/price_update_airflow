from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import sys, os


PROJECT_DIR = "/home/ubuntu/airflow-project/price_update_airflow"
sys.path.append(PROJECT_DIR)
sys.path.append(os.path.join(PROJECT_DIR, "config"))

from load_csv_to_db import processing_csv
from config.config import SQLALCHEMY_DATABASE_URI

CSV_PATH = os.path.join(PROJECT_DIR, "output", "price_update.csv")


default_args = {
    'owner': 'price_user',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 12),
    'retry_delay': timedelta(minutes = 5)
}

with DAG(
            'update_db_price',
            default_args = default_args,
            schedule_interval='@daily',
            catchup=False,
            max_active_tasks=3,
            max_active_runs=1,
            tags=['price_update']
) as dag:
            

  load_db_data = PythonOperator(
      task_id='load_data_db',
      python_callable=processing_csv,
      op_args=[CSV_PATH, SQLALCHEMY_DATABASE_URI],
  )