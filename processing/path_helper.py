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

    # Завантажує файл з S3 (raw/current) і повертає BytesIO-обєкт
    key = f'raw/current/{filename}'

    response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    file_content = response['Body'].read()

    print(f'Файл {key} успішно зчитано з S3 bucket')
    return BytesIO(file_content)