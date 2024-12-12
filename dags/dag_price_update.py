from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from processing import main_outfit
from for_update import download_file, add_for_update, create_price_xlsx
from load_csv_to_db import processing_csv
from load_sale_file import download_file_from_s3, load_excel_to_database
from join_table_load_s3 import fetch_data_from_db, process_data, upload_to_s3
from main import functions_list

import sys
sys.path.append("..")
from config.config import (link_list, SQLALCHEMY_DATABASE_URI, DB_POSTGRES_CONFIG, AWS_ACCESS_KEY_ID,
                           AWS_SECRET_ACCESS_KEY)

# Конфігурація
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Дані для обробки
csv_file_path = "price_update.csv"
DATABASE_URL = SQLALCHEMY_DATABASE_URI
DB_CONFIG = DB_POSTGRES_CONFIG
S3_CONFIG = {
    'bucket_name': 'for-price-update-bucket',
    'aws_access_key_id': AWS_ACCESS_KEY_ID,
    'aws_secret_access_key': AWS_SECRET_ACCESS_KEY
}


with DAG(
    dag_id='price_update_pipeline',
    default_args=default_args,
    description='Pipeline for processing price updates',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Завдання 1. Завантаження прайсів
    download_task = PythonOperator(
        task_id='download_files',
        python_callable=lambda: [
            download_file(url_link, name_link) for url_link, name_link in link_list
        ]
    )

    # Таск 2. Обробка основного прайсу для порівняння з іншими
    outfit_task = PythonOperator(
        task_id='process_outfit',
        python_callable=outfit_file_operation,
        execution_timeout=timedelta(minutes=10),
        do_xcom_push=True  # зберігати результати в XCom
    )

    # Таск 3. Додавання результатів обробки прайсів до порівняння
    def add_update_data(**kwargs):
        # отрммуємо дані з XCom
        data_for_update = kwargs['ti'].xcom_pull(task_ids='process_outfit')

        for name_func, func in functions_list:
            result_dict = func()
            add_for_update(name_func, result_dict, data_for_update)

    # Таск 5. Створення xlsx файлу
    create_price_task = PythonOperator(
        task_id='create_price_xlsx',
        python_callable=create_price_xlsx,
        op_kwargs={'data_for_update': main_outfit.outfit_file_operation()}
    )

    # Завдання 6. Обробка CSV файлу для бази даних
    process_csv_task = PythonOperator(
        task_id='process_csv',
        python_callable=processing_csv,
        op_kwargs={'file_path': csv_file_path, 'database_url': DATABASE_URL}
    )

    # Завдання 7. Завантаження файлу зі знижками до DB
    download_s3_task = PythonOperator(
        task_id='download_sale_file',
        python_callable=download_file_from_s3,
        op_kwargs={
            'bucket_name': S3_CONFIG['bucket_name'],
            'file_name': 'sale.xlsx',
            'local_file_path': 'sale.xlsx'
        }
    )

    # Завдання 8. Завантаження файлу з оновленими прайсами до DB
    load_excel_task = PythonOperator(
        task_id='load_excel_to_db',
        python_callable=load_excel_to_database,
        op_kwargs={'file_path': 'sale.xlsx', 'db_url': DATABASE_URL}
    )

    # Завдання 9. Обробка файлу в Data Bas
    process_db_task = PythonOperator(
        task_id='process_db_data',
        python_callable=lambda: process_data(fetch_data_from_db(DB_CONFIG))
    )

    # Завдання 10. Завантаження оновленого прайсу з доданими знижками до S3 bucket
    upload_s3_task = PythonOperator(
        task_id='upload_clean_data_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'data': process_data(fetch_data_from_db(DB_CONFIG)),
            'config': S3_CONFIG
        }
    )


    download_task >> outfit_task >> add_update_task >> create_price_task >> process_csv_task
    process_csv_task >> download_s3_task >> load_excel_task >> process_db_task >> upload_s3_task
