from processing import main_adr, main_atl, main_dasmart, main_dosp, main_kemp, main_norf, main_outfit, main_shamb, \
    main_swa, main_trp
import os
from datetime import datetime
import pandas as pd
import requests
import shutil
import sys
sys.path.append("..")
from config.config import link_list

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PRICE_DIR = os.path.join(CURRENT_DIR, "price")
CURRENT_FOLDER = os.path.join(PRICE_DIR, "current")
PREVIOUS_FOLDER = os.path.join(PRICE_DIR, "previous")

# створюємо папки якщо їх ще немає
os.makedirs(CURRENT_FOLDER, exist_ok=True)
os.makedirs(PREVIOUS_FOLDER, exist_ok=True)


def download_file(url, name):
    # file_path = os.path.join(CURRENT_DIR, "price", name)
    current_file = os.path.join(CURRENT_FOLDER, name)
    previous_file = os.path.join(PREVIOUS_FOLDER, name)

    # якщо є поточний файл переносимо його в previous
    if os.path.exists(current_file):
        shutil.copyfile(current_file, previous_file)

    # виконати запит GET до сервера та отримати відповідь
    try:
        response = requests.get(url)
        response.raise_for_status()

        # зберегти вміст відповіді в файл
        with open(current_file, 'wb') as file:
            file.write(response.content)

        print(f'Файл {name} було успішно завантажено та перезаписано в current.')

    except requests.exceptions.RequestException as e:
        print(f'Виникла помилка під час завантаження {name}: {str(e)}')

        # якщо є резервна версія - відновлюємо файл з previous
        if os.path.exists(previous_file):
            shutil.copyfile(previous_file, current_file)
            print(f'↩️ Файл {name} відновлено з попередньої версії')
        else:
            print(f'❌ Немає попереднього файлу {name}, відновлення неможливе.')



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
