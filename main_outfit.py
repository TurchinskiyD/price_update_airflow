import openpyxl


def outfit_file_operation():
    # завантажити книгу Excel з файлу
    wb = openpyxl.load_workbook(filename="price/outfitter.xlsx")

    # отримати активний аркуш
    ws = wb.active

    # створити порожній словник для зберігання даних
    data_outfitter = {}

    # прочитати дані з кожного рядка (крім першого, який містить заголовки стовпців)
    for row in ws.iter_rows(min_row=2, values_only=True):
        # створити словник з даних рядка
        offer_data = {"available": "Немає в наявності", "price": row[9], "name": row[3]}

        data_outfitter[str(row[0])] = offer_data

    return data_outfitter


# print(outfit_file_operation())
