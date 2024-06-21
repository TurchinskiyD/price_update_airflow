import xml.etree.ElementTree as ET


def dosp_file_operation():

    tree_dosp = ET.parse('price/dospehi.xml')
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

