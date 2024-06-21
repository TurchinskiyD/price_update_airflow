import xml.etree.ElementTree as ET


def dasmart_file_operation():

    tree = ET.parse('price/dasmart.xml')
    root = tree.getroot()

    data = {}

    for offer in root.findall('./shop/offers/offer'):

        vendor_code = offer.find('vendorCode').text
        offer_data = {}

        if offer.get('available') == "true":
            offer_data['available'] = "В наявності"
        else:
            offer_data['available'] = 'Немає в наявності'

        offer_data['price'] = float(offer.find('price').text)
        offer_data['name'] = offer.find('name').text

        data[vendor_code] = offer_data  # Додати дані до словника data

    return data

