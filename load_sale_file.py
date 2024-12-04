
import boto3
import pandas as pd
from sqlalchemy import create_engine

import sys
sys.path.append("..")
from config import SQLALCHEMY_DATABASE_URI, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

DB_CONNECTION_STRING = SQLALCHEMY_DATABASE_URI

# Налаштування S3
BUCKET_NAME = 'for-sales-bucket'
FILE_NAME = 'sale.xlsx'
LOCAL_FILE_PATH = 'sale.xlsx'


def download_file_from_s3(bucket_name, file_name, local_path):
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3.download_file(bucket_name, file_name, local_path)
    print(f"Файл {file_name} завантажено з S3 до {local_path}")





download_file_from_s3(BUCKET_NAME, FILE_NAME, LOCAL_FILE_PATH)