import openpyxl
import os


def swa_file_operation():

    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "../price/swa.xlsx")
    # завантажити книгу Excel з файлу
    wb = openpyxl.load_workbook(file_path)

    # отримати активний аркуш
    ws = wb.active

    # створити порожній словник для зберігання даних
    data_swa = {}

    # прочитати дані з кожного рядка (крім першого, який містить заголовки стовпців)
    for row in ws.iter_rows(min_row=2, values_only=True):
        # створити словник з даних рядка
        item_data = {"available": row[11], "price": row[7]}

        # додати словник до словника даних, використовуючи артикул як ключ
        data_swa[str(row[0])] = item_data

    return data_swa


# print(swa_file_operation())


