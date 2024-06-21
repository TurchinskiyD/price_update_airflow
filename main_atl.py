import xml.etree.ElementTree as ET


def atl_file_operation():

    tree = ET.parse('price/atlantmarket.xml')
    root = tree.getroot()

    data = {}

    for offer in root.findall('./shop/offers/offer'):

        vendor_code = offer.find('barcode').text
        offer_data = {}

        if offer.get('available') == "true":
            offer_data['available'] = "В наявності"
        elif offer.get('available') == "false":
            offer_data['available'] = 'Немає в наявності'

        offer_data['price'] = float(offer.find('price').text)
        offer_data['name'] = offer.find('name').text

        data[vendor_code] = offer_data  # Додати дані до словника data

    return data


# print(atl_file_operation())

# def update_viter_in_atl(dict_updated, dict_updater):
#     counter = 0
#     for key, value in dict_updater.items():
#         if key in dict_updated:
#             if dict_updated[key]['available'] == 'Немає в наявності' and dict_updated[key]['available'] == 'В наявності':
#
#                 try:
#                     data_for_update[key]['available'] = value['available']
#                     data_for_update[key]['price'] = value['price']
#                 counter += 1
#             except Exception as e:
#                 print(f"Помилка при обробці словника {name}: {e}")
#                 break
#     print(f"В каталозі Аутфіттер оновлено {counter} товарів. "
#           f"Загльна кількість товарів в прайсі {name} {len(dictionary)} елементів.")










