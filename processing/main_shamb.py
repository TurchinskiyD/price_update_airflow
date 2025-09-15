import xlrd
import os
from processing.path_helper import get_price_file_path


def shamb_file_operation(file_name = 'shambala.xls'):
    # base_dir = os.path.dirname(os.path.abspath(__file__))
    # file_path = os.path.join(base_dir, "../price/shambala.xls")
    file_path = get_price_file_path(file_name)

    # завантажити книгу Excel з файлу
    workbook = xlrd.open_workbook(file_contents=file_path.read())

    # отримати активний аркуш
    worksheet = workbook.sheet_by_index(0)

    # створити порожній словник для зберігання даних
    data_shamb = {}

    # прочитати дані з кожного рядка (крім перших 6, який містить заголовки стовпців)
    for row in range(6, worksheet.nrows):
        # отримати ключ (артикул товару) з комірки таблиці
        key = str(worksheet.cell_value(row, 4))

        if key != "":

            if worksheet.cell_value(row, 5) or worksheet.cell_value(row, 6) or worksheet.cell_value(row, 7):
                available = 'В наявності'
            else:
                available = 'Немає в наявності'

            if worksheet.cell_value(row, 10) != '':
                price = float(worksheet.cell_value(row, 10))
            else:
                price = 0.0

            # створити словник з даних рядка
            item_data = {"available": available, "price": price}

            data_shamb[key] = item_data
    return data_shamb


# print(shamb_file_operation())


