from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from model.price_load import Price
import pandas as pd

import sys
sys.path.append("..")
from config import SQLALCHEMY_DATABASE_URI

# Підключення до бази даних
DATABASE_URL = SQLALCHEMY_DATABASE_URI
csv_file_path = "price_update.csv"


def load_csv_to_db(file_path, db_connection):
    engine = create_engine(db_connection)
    Price.metadata.create_all(engine)  # Автоматично створює таблиці, якщо їх ще немає
    Session = sessionmaker(bind=engine)
    session = Session()

    # Читання даних із CSV
    df = pd.read_csv(file_path)

    # Очищення таблиці
    try:
        session.query(Price).delete()
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
            new_record = Price(
                product_id=row['Артикул'],
                status=row['Наличие'],
                current_price=row['Цена'],
                product_name=row['Назва']
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


load_csv_to_db(csv_file_path, DATABASE_URL)
