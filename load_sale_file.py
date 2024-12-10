from sqlalchemy.orm import sessionmaker
import boto3
import pandas as pd
from sqlalchemy import create_engine
from model.sale_load import Sale

import sys
sys.path.append("..")
from config.config import SQLALCHEMY_DATABASE_URI, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

# DB_CONNECTION_STRING = SQLALCHEMY_DATABASE_URI
#
# # Налаштування S3
# BUCKET_NAME = 'for-sales-bucket'
# FILE_NAME = 'sale.xlsx'
# LOCAL_FILE_PATH = 'sale.xlsx'


def download_file_from_s3(bucket_name, file_name, local_path):
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
    s3.download_file(bucket_name, file_name, local_path)
    print(f"Файл {file_name} завантажено з S3 до {local_path}")


def load_excel_to_database(file_path, db_connection_string):
    # Зчитати Excel-файл
    df = pd.read_excel(file_path)

    # Видалення дублікатів за колонкою product_id
    df = df.drop_duplicates(subset=['Артикул'], keep='last')

    # Видалення записів з відсутніми product_id
    df = df.dropna(subset=['Артикул'])

    # Очищення та підготовка даних
    df['Знижка'] = df['Знижка'].fillna(0).astype(int)  # Замінюємо NaN на 0 та перетворюємо на int

    # Перевірити структуру даних
    print("Зчитані дані:")
    print(df.head())


    # Підключення до бази даних
    engine = create_engine(db_connection_string)
    Sale.metadata.create_all(engine)  # Автоматично створює таблиці, якщо їх ще немає
    Session = sessionmaker(bind=engine)
    session = Session()

    # Очищення таблиці
    try:
        session.query(Sale).delete()
        session.commit()
        print("Таблицю успішно очищено.")
    except Exception as e:
        session.rollback()
        print(f"Помилка при очищенні таблиці: {e}")
        session.close()
        raise

    # Додавання нових записів
    try:
        for index, row in df.iterrows():
            new_record = Sale(
                product_id=row['Артикул'],
                sale=row['Знижка'],
            )
            session.add(new_record)

        # Збереження змін
        session.commit()
        print("Дані успішно додано до таблиці.")
    except Exception as e:
        session.rollback()
        print(f"Помилка при записі даних: {e}")
    finally:
        session.close()
