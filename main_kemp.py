import xlrd

# завантажити книгу Excel з файлу
def kemping_file_operation():
    work_book = xlrd.open_workbook("price/kemping.xls")

    # отримати активний аркуш
    work_sheet = work_book.sheet_by_index(0)

    # створити порожній словник для зберігання даних
    data_kemp = {}

    # прочитати дані з кожного рядка (крім перших пяти, який містить заголовки стовпців)
    for row in range(5, work_sheet.nrows):
        # отримати ключ (артикул товару) з комірки таблиці
        key = work_sheet.cell_value(row, 0)
        if key != '':

            # створити словник з даних рядка
            if work_sheet.cell_value(row, 6) == '' or int(work_sheet.cell_value(row, 6)) > 0:
                available = 'В наявності'
            else:
                available = 'Немає в наявності'

            item_data = {"available": available, "price": work_sheet.cell_value(row, 9)}

            data_kemp[str(int(key))] = item_data

    return data_kemp

# print(kemping_file_operation())
