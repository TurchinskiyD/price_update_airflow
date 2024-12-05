import pandas as pd
import boto3
import psycopg2
from io import BytesIO
from sqlalchemy import create_engine

import sys
sys.path.append("..")
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, DB_POSTGRES_CONFIG, SQLALCHEMY_DATABASE_URI

# Конфігурація бази даних та S3
DB_CONFIG = DB_POSTGRES_CONFIG

S3_CONFIG = {
    'bucket_name': 'for-price-update-bucket',
    'aws_access_key_id': AWS_ACCESS_KEY_ID,
    'aws_secret_access_key': AWS_SECRET_ACCESS_KEY
}


# Функція для отримання даних з бази даних
def fetch_data_from_db():
    query = """
        SELECT pu.product_id, pu.status, pu.current_price, pu.product_name, ps.sale
        FROM prod_update pu
        LEFT JOIN prod_sale ps ON pu.product_id = ps.product_id;
    """
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            return pd.read_sql_query(query, conn)
    except Exception as e:
        print("Error fetching data from database:", e)
        raise


# Функція для збереження файлів у S3
def upload_to_s3(dataframe, filename, file_format):
    s3 = boto3.client(
        's3',
        aws_access_key_id=S3_CONFIG['aws_access_key_id'],
        aws_secret_access_key=S3_CONFIG['aws_secret_access_key']
    )

    buffer = BytesIO()

    if file_format == 'csv':
        dataframe.to_csv(buffer, index=False)
    elif file_format == 'xlsx':
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            dataframe.to_excel(writer, index=False, sheet_name='Sheet1')
    else:
        raise ValueError("Unsupported file format")

    buffer.seek(0)

    s3.upload_fileobj(buffer, S3_CONFIG['bucket_name'], filename)
    print(f"Uploaded {filename} to S3")


# Основна функція
def main():
    try:
        print("Fetching data from database...")
        df = fetch_data_from_db()

        print("Uploading CSV file to S3...")
        upload_to_s3(df, 'prod_data.csv', 'csv')

        print("Uploading XLSX file to S3...")
        upload_to_s3(df, 'prod_data.xlsx', 'xlsx')

        print("Process completed successfully!")
    except Exception as e:
        print("Error in processing:", e)


if __name__ == "__main__":
    main()
