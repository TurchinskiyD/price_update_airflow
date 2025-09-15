import boto3
import botocore
from datetime import datetime
import pandas as pd
import requests
import os
from config.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME, REGION_NAME

s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION_NAME
)


def download_file(url: str, name: str):

    current_key = f'raw/current/{name}'
    previous_key = f'raw/previous/{name}'

    try:
        # якщо є файл в current - копіюємо в previous
        try:
            s3.head_object(
                Bucket=BUCKET_NAME,
                Key=current_key
            )
            s3.copy_object(
                Bucket=BUCKET_NAME,
                CopySource={"Bucket": BUCKET_NAME, "Key": current_key},
                Key=previous_key
            )
            print(f'🔄 Файл {name} перенесено з current до previous')
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                print(f'ℹ️ Файлу {name} ще немає в current, пропускаємо копіювання')
            else:
                raise

        # качаємо "свіжий" прайс
        response = requests.get(url)
        response.raise_for_status()

        # завантажуємо в папку current
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=current_key,
            Body=response.content
        )
        print(f'✅ Файл {name} завантажено до s3://{BUCKET_NAME}/{current_key}')

    except Exception as e:
        print(f'❌ Помилка при завантаженні {name}: {str(e)}')

        # fallback: якщо помилка при завантаженні в current і є файл в previous намагаємося відновити файл з previous
        try:
            s3.head_object(
                Bucket=BUCKET_NAME,
                Key=current_key
            )
            s3.copy_object(
                Bucket=BUCKET_NAME,
                CopySource={"Bucket": BUCKET_NAME, "Key": previous_key},
                Key=current_key
            )
            print(f'↩️ Файл {name} відновлено з previous')
        except botocore.exceptions.ClientError:
            print(f'❌ Немає резервного файлу {name} в previous')



def add_for_update(name, dictionary, dict_for_update):
    dict_temp = dictionary
    counter = 0
    for key, value in dict_temp.items():
        if key in dict_for_update:
            try:
                dict_for_update[key]['available'] = value['available']
                dict_for_update[key]['price'] = value['price']
                counter += 1
            except Exception as e:
                print(f"Помилка при обробці словника {name}: {e}")
                break
    print(f"В каталозі Аутфіттер оновлено {counter} товарів. "
          f"Загльна кількість товарів в прайсі {name} {len(dictionary)} елементів.")



def create_price_xlsx(dict_for_update):
    data = [
        {
            'Артикул': key,
            'Наличие': item['available'],
            'Цена': item['price'],
            'Назва': item['name']
        }
        for key, item in dict_for_update.items()
    ]

    # Створюємо DataFrame
    df = pd.DataFrame(data)

    # Форматуємо дату і час для назви файлів
    now = datetime.now()
    formatted_time = now.strftime("%Y_%m_%d_%H_%M")
    
    # Формуємо шлях для збереження файлу в проєкті
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    OUTPUT_DIR = os.path.join(BASE_DIR, "output")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Зберігаємо у форматах Excel і CSV
    excel_file = os.path.join(OUTPUT_DIR, f"price_update_{formatted_time}.xlsx")
    csv_file = os.path.join(OUTPUT_DIR, f"price_update.csv")

    df.to_excel(excel_file, index=False)  # Збереження у форматі Excel
    df.to_csv(csv_file, index=False, encoding='utf-8-sig')  # Збереження у форматі CSV (UTF-8 для кирилиці)

    print(f"Файли {excel_file} та {csv_file} збережено у {OUTPUT_DIR}")
