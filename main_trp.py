import openpyxl


def trp_file_operation():

    # завантажити книгу Excel з файлу
    wb = openpyxl.load_workbook(filename="price/tramp.xlsx")

    # отримати активний аркуш
    ws = wb.active

    # створити порожній словник для зберігання даних
    data_trp = {}

    # прочитати дані з кожного рядка (крім першого, який містить заголовки стовпців)
    for row in ws.iter_rows(min_row=2, values_only=True):
        # створити словник з даних рядка
        __ex_rate = 42
        price = float(row[9]) * __ex_rate
        item_data = {"available": row[12], "price": price}

        # додати словник до словника даних, використовуючи артикул як ключ
        data_trp[str(row[0])] = item_data

    return data_trp


# print(trp_file_operation())
