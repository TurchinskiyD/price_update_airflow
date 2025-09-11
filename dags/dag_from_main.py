from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

import sys

sys.path.append("/price_update_airflow")

bash_command = Variable.get("PRICE_UPDATE_COMMAND")


def notify_email(context):
    to = [Variable.get("alert_email")]
    subject = f"Airflow alert: {context['task_instance'].task_id} failed"
    body = f"""
            DAG: {context['dag'].dag_id}<br>
            Task: {context['task_instance'].task_id}<br>
            Execution Time: {context['execution_date']}<br>
            <a href="{context['task_instance'].log_url}">View log</a>
        """
    send_email(to=to, subject=subject, html_content=body)


def PythonOp(*args, **kwargs):
    kwargs["on_failure_callback"] = notify_email
    return PythonOperator(*args, **kwargs)


def BashOp(*args, **kwargs):
    kwargs["on_failure_callback"] = notify_email
    return BashOperator(*args, **kwargs)


default_args = {
    'owner': 'price_user',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 12),
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('update_from_main',
          default_args=default_args,
          schedule_interval='@daily',
          catchup=False,
          max_active_tasks=3,
          max_active_runs=1,
          tags=['price_update'])

extract_data_and_load = BashOp(
    task_id='extract_data_and_load',
    bash_command=bash_command,
    dag=dag
)


# def fail_task():
#     raise Exception('Тестова помилка для перевірки сповіщень')
#
#
# test_fail = PythonOp(
#     task_id="test_fail",
#     python_callable=fail_task,
# )

extract_data_and_load #>> test_fail
