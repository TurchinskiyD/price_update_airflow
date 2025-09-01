import xml.etree.ElementTree as ET
import os
from processing.path_helper import get_price_file_path


def dosp_file_operation(file_name = 'dospehi.xml'):
    # base_dir = os.path.dirname(os.path.abspath(__file__))
    # file_path = os.path.join(base_dir, '../price/dospehi.xml')
    file_path = get_price_file_path(file_name)

    tree_dosp = ET.parse(file_path)
    root_dosp = tree_dosp.getroot()

    data_dosp = {}

    for offer_dosp in root_dosp.findall('./shop/offers/offer'):
        vendor_code_dosp = offer_dosp.find('vendorCode').text
        offer_data_dosp = {}

        if offer_dosp.get('available') == "true":
            offer_data_dosp['available'] = "В наявності"
        else:
            offer_data_dosp['available'] = 'Немає в наявності'

        offer_data_dosp['price'] = float(offer_dosp.find('price').text)
        offer_data_dosp['name'] = offer_dosp.find('name').text

        data_dosp[vendor_code_dosp] = offer_data_dosp

    return data_dosp


# print(dosp_file_operation())

