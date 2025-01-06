import os
import sys
import json
import pandas as pd

# Aбсолютний шлях до проекту
sys.path.append('/price_update_airflow')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from processing.main_outfit import outfit_file_operation
from for_update import download_file, add_for_update, create_price_xlsx
from load_csv_to_db import processing_csv
from load_sale_file import download_file_from_s3, load_excel_to_database
from join_table_load_s3 import fetch_data_from_db, process_data, upload_to_s3
from main import functions_list

sys.path.append("..")
from config.config import (link_list, SQLALCHEMY_DATABASE_URI, DB_POSTGRES_CONFIG, AWS_ACCESS_KEY_ID,
                           AWS_SECRET_ACCESS_KEY)

# Дані для обробки
csv_file_path = "/data/price_update.csv"
sale_file_path = "/data/sale.xlsx"
DATABASE_URL = SQLALCHEMY_DATABASE_URI
DB_CONFIG = DB_POSTGRES_CONFIG
S3_CONFIG = {
    'bucket_name': 'for-price-update-bucket',
    'aws_access_key_id': AWS_ACCESS_KEY_ID,
    'aws_secret_access_key': AWS_SECRET_ACCESS_KEY
}
BUCKET_NAME = 'for-sales-bucket'
FILE_NAME = 'sale.xlsx'


# Конфігурація
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='price_update_pipeline',
    default_args=default_args,
    description='Pipeline for processing price updates',
    schedule_interval='@daily',
    start_date=datetime(2024, 12, 12),
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
        ti = kwargs['ti']
        data_for_update = ti.xcom_pull(task_ids='process_outfit')

        # Перевірка, чи є отримані дані
        if not data_for_update:
            raise ValueError("Дані для оновлення відсутні в XCom")

        # Оновлюємо дані
        for name_func, func in functions_list:
            result_dict = func()
            add_for_update(name_func, result_dict, data_for_update)

        # Приведення типів поля 'price' перед збереженням у XCom
        for key, item in data_for_update.items():
            if 'price' in item:
                # Перевіряємо, чи значення 'price' є рядком і перетворюємо його на float
                if isinstance(item['price'], str):
                    try:
                        item['price'] = float(item['price'].replace(',', '.'))
                    except ValueError:
                        raise ValueError(f"Неправильний формат ціни для артикулу {key}: {item['price']}")

            # Зберігаємо оновлені дані в XCom для наступного таска
        ti.xcom_push(key='updated_data', value=data_for_update)


    add_update_task = PythonOperator(
        task_id='add_update_data',
        python_callable=add_update_data,
        provide_context=True  # дозволяє передати контекст XCom
    )

    # Таск 4. Створення xlsx файлу
    def prepare_data_for_create_xlsx(**kwargs):
        ti = kwargs['ti']
        # Отримуємо дані з XCom
        raw_data = ti.xcom_pull(task_ids='add_update_data', key='updated_data')

        # Перевіряємо, чи дані є рядком, і перетворюємо їх на словник
        if isinstance(raw_data, str):
            dict_for_update = json.loads(raw_data)
        else:
            dict_for_update = raw_data

        # Викликаємо функцію з передачею словника
        create_price_xlsx(dict_for_update)


    prepare_xlsx_task = PythonOperator(
        task_id='prepare_data_for_create_xlsx',
        python_callable=prepare_data_for_create_xlsx,
        provide_context=True
    )

    # Завдання 5 Завантаження файлу з оновленими прайсами до DB
    process_csv_task = PythonOperator(
        task_id='process_csv',
        python_callable=processing_csv,
        op_kwargs={'file_path': csv_file_path, 'database_url': DATABASE_URL}
    )

    # Завдання 6. Завантаження файлу зі знижками з S3 bucket
    download_s3_task = PythonOperator(
        task_id='download_sale_file',
        python_callable=download_file_from_s3,
        op_kwargs={
            'bucket_name': BUCKET_NAME,
            'file_name': FILE_NAME,
            'local_file_path': sale_file_path
        }
    )

    # Завдання 7. Завантаження файлу зi знижками до DB
    load_excel_task = PythonOperator(
        task_id='load_excel_to_db',
        python_callable=load_excel_to_database,
        op_kwargs={'file_path': sale_file_path, 'db_connection_string': DATABASE_URL}
    )

    # Завдання 8. Об'єднання двох таблиць та отримання результату
    process_db_task = PythonOperator(
        task_id='process_db_data',
        python_callable=fetch_data_from_db,
        op_kwargs={'db_conn_config': DB_CONFIG}
    )

    # Завдання 9. Очистка отриманого з бази даних результату
    def prepare_join_data(**kwargs):
        ti = kwargs['ti']
        # Отримуємо дані з XCom
        raw_data = ti.xcom_pull(task_ids='process_db_data')
        print(f"Type of data received from XCom: {type(raw_data)}")

        # Перевірка типу
        if isinstance(raw_data, pd.DataFrame):
            df = raw_data
        else:
            raise ValueError(f"Unexpected data type received from XCom: {type(raw_data)}")

        # Обробка даних
        processed_df = process_data(df)
        print(processed_df.head())

        # Явне повернення оброблених даних, щоб вони збереглися в XCom
        return processed_df


    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=prepare_join_data
    )

    # Завдання 10. Завантаження оновленого прайсу з доданими знижками до S3 bucket
    def upload_clean_data_to_s3(**context):
        ti = context['ti']
        dataframe = ti.xcom_pull(task_ids="clean_data")

        if isinstance(dataframe, pd.DataFrame):
            df = dataframe
        else:
            raise ValueError(f"Unexpected data type received from XCom: {type(dataframe)}")

        upload_to_s3(df, S3_CONFIG)

    upload_s3_task = PythonOperator(
        task_id='upload_clean_data_to_s3',
        python_callable=upload_clean_data_to_s3
    )

    download_task >> outfit_task >> add_update_task >> prepare_xlsx_task >> process_csv_task
    process_csv_task >> download_s3_task >> load_excel_task >> process_db_task >> clean_data_task >> upload_s3_task