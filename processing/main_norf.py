import xlrd
import os
from processing.path_helper import get_price_file_path


def norf_file_operation(file_name ='norfin.xls'):

    # base_dir = os.path.dirname(os.path.abspath(__file__))
    # file_path = os.path.join(base_dir, "../price/norfin.xls")
    file_path = get_price_file_path(file_name)

    # завантажити книгу Excel з файлу
    workbook = xlrd.open_workbook(file_contents=file_path.read())

    # отримати активний аркуш
    worksheet = workbook.sheet_by_index(0)

    # створити порожній словник для зберігання даних
    data_norf = {}

    # прочитати дані з кожного рядка (крім першого, який містить заголовки стовпців)
    for row in range(16, worksheet.nrows):
        # створити словник з даних рядка

        if worksheet.cell_value(row, 3) == "Да" or worksheet.cell_value(row, 3) == 'Нет':
            available = 'В наявності'
        else:
            available = 'Немає в наявності'

        item_data = {"available": available, "price": worksheet.cell_value(row, 9)}
        data_norf[str(worksheet.cell_value(row, 1))] = item_data

    return data_norf


#print(norf_file_operation())

