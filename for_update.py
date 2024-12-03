import os
from datetime import datetime
import pandas as pd
import requests
from processing import main_adr, main_atl, main_dasmart, main_dosp, main_kemp, main_norf, main_outfit, main_shamb, \
    main_swa, main_trp


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


data_for_update = main_outfit.outfit_file_operation()


def add_for_update(name, dictionary):
    dict_temp = dictionary
    counter = 0
    for key, value in dict_temp.items():
        if key in data_for_update:
            try:
                data_for_update[key]['available'] = value['available']
                data_for_update[key]['price'] = value['price']
                counter += 1
            except Exception as e:
                print(f"Помилка при обробці словника {name}: {e}")
                break
    print(f"В каталозі Аутфіттер оновлено {counter} товарів. "
          f"Загльна кількість товарів в прайсі {name} {len(dictionary)} елементів.")


def create_price_xlsx(data_outfitter):
    data = [
        {
            'Артикул': key,
            'Наличие': item['available'],
            'Цена': item['price'],
            'Назва': item['name']
        }
        for key, item in data_outfitter.items()
    ]

    # Створюємо DataFrame
    df = pd.DataFrame(data)

    # Форматуємо дату і час для назви файлів
    now = datetime.now()
    formatted_time = now.strftime("%Y_%m_%d_%H_%M")

    # Зберігаємо у форматах Excel і CSV
    excel_file = f"price_update_{formatted_time}.xlsx"
    csv_file = f"price_update_{formatted_time}.csv"

    df.to_excel(excel_file, index=False)  # Збереження у форматі Excel
    df.to_csv(csv_file, index=False, encoding='utf-8-sig')  # Збереження у форматі CSV (UTF-8 для кирилиці)

    print(f"Файли збережено: {excel_file}, {csv_file}")


if __name__ == "__main__":

    link_list = [('https://outfitter.in.ua/content/export/1dc57c3051db9aa40919d7d71ef7b23e.xlsx', 'outfitter.xlsx'),
                 ('https://www.dropbox.com/s/qzg0r695jx3m8iw/rozn.XLS?dl=1', 'shambala.xls'),
                 ('https://tramp.ua/content/export/wholesale/tramp.ua_8f14e45fceea167a5a36dedd4bea2543.xlsx',
                  'tramp.xlsx'),
                 ('https://uabest.com.ua/content/export/36.xml', 'dospehi.xml'),
                 ('https://atlantmarket.com.ua/price1/prom/atlantmarketprom(false).xml', 'atlantmarket.xml'),
                 ('https://opt.adrenalin.od.ua/Adrenalin_stock.xml', 'adrenalin.xml'),
                 ('https://sva.com.ua/content/export/80.xlsx', 'swa.xlsx'),
                 ('https://www.dasmart.com.ua/content/export/e7052aba0549f32c1a825bf48539397b.xml', 'dasmart.xml')]

    for url_link, name_link in link_list:
        download_file(url_link, name_link)

    print(f'Кількість товарів в каталозі Аутфіттер {len(data_for_update)}')

    functions_list = [("Атлант", main_atl.atl_file_operation),
                      ("Кемпінг", main_kemp.kemping_file_operation),
                      ("Шамбала", main_shamb.shamb_file_operation),
                      ("Трамп", main_trp.trp_file_operation),
                      ("Сва", main_swa.swa_file_operation),
                      ("Доспехи", main_dosp.dosp_file_operation),
                      ("Норфін", main_norf.norf_file_operation),
                      ("Адреналін", main_adr.adr_file_operation),
                      ("Дасмарт", main_dasmart.dasmart_file_operation)]

    for name_func, func in functions_list:
        result_dict = func()
        add_for_update(name_func, result_dict)

    create_price_xlsx(data_for_update)