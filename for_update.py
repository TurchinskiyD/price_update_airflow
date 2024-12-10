import os
from datetime import datetime
import pandas as pd
import requests


def download_file(url, name):
    file_path = os.path.join("price/", name)

    # виконати запит GET до сервера та отримати відповідь
    try:
        response = requests.get(url)
        response.raise_for_status()

        # зберегти вміст відповіді в файл
        with open(file_path, 'wb') as file:
            file.write(response.content)

        print(f'Файл {name} було успішно завантажено та перезаписано.')

    except requests.exceptions.RequestException as e:
        print(f'Виникла помилка під час завантаження {name}: {str(e)}')



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

    # Визначаємо папку для збереження файлів
    output_folder = "data"
    os.makedirs(output_folder, exist_ok=True)  # Створюємо папку, якщо її не існує

    # Формуємо шляхи для файлів
    excel_file = os.path.join(output_folder, f"price_update_{formatted_time}.xlsx")
    csv_file = os.path.join(output_folder, "price_update.csv")


    df.to_excel(excel_file, index=False)  # Збереження у форматі Excel
    df.to_csv(csv_file, index=False, encoding='utf-8-sig')  # Збереження у форматі CSV (UTF-8 для кирилиці)

    print(f"Файли збережено: {excel_file}, {csv_file}")