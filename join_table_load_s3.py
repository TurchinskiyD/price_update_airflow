from datetime import datetime

import pandas as pd
import boto3
import psycopg2
from io import BytesIO

import sys
sys.path.append("..")
from config.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, DB_POSTGRES_CONFIG, SQLALCHEMY_DATABASE_URI

# Конфігурація бази даних та S3
DB_CONFIG = DB_POSTGRES_CONFIG

S3_CONFIG = {
    'bucket_name': 'for-price-update-buck',
    'aws_access_key_id': AWS_ACCESS_KEY_ID,
    'aws_secret_access_key': AWS_SECRET_ACCESS_KEY
}


# Функція для отримання даних з бази даних
def fetch_data_from_db(db_conn_config):
    query = """
        SELECT pu.product_id, pu.status, pu.current_price, pu.product_name, ps.sale
        FROM prod_update pu
        LEFT JOIN prod_sale ps ON pu.product_id = ps.product_id;
    """
    try:
        with psycopg2.connect(**db_conn_config) as conn:
            return pd.read_sql_query(query, conn)
    except Exception as e:
        print("Error fetching data from database:", e)
        raise


# Функція для збереження файлів у S3
def upload_to_s3(dataframe, s3_conn_config):

    s3 = boto3.client(
        's3',
        aws_access_key_id=s3_conn_config['aws_access_key_id'],
        aws_secret_access_key=s3_conn_config['aws_secret_access_key']
    )

    buffer = BytesIO()

    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        dataframe.to_excel(writer, index=False, sheet_name='Sheet1')

    buffer.seek(0)

    now = datetime.now()
    formatted_time = now.strftime("%Y_%m_%d_%H_%M")
    filename = f'update_with_sale_{formatted_time}.xlsx'

    s3.upload_fileobj(buffer, S3_CONFIG['bucket_name'], filename)
    print(f"Uploaded {filename} to S3")


def process_data(df):
    # Перейменування колонок
    df.columns = ['Артикул', 'Наявність', 'Ціна', 'Назва', 'Знижка']

    # Перетворення 'Ціна' в числовий формат
    df['Ціна'] = pd.to_numeric(df['Ціна'], errors='coerce')

    # Замінити порожні значення у 'Знижка' на 0
    df['Знижка'] = df['Знижка'].fillna(0)

    return df

