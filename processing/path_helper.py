import os
import boto3
from io import BytesIO

from config.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME, REGION_NAME

s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION_NAME
)


def get_price_file_path(filename: str) -> BytesIO:
    # """
    # Повертає абсолютний шлях до файлу прайсу в папці price/current
    # """
    # base_dir = os.path.dirname(os.path.abspath(__file__))
    # return os.path.join(base_dir, "../price/current", filename)

    # Завантажуємо файл з S3 (raw/current) у память та повертаємо BytesIO-об`єкт
    key = f'raw/current/{filename}'

    response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    file_content = response['Body'].read()

    print(f'Файл {key} успішно зчитано з S3 bucket')
    return BytesIO(file_content)