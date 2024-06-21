import xml.etree.ElementTree as ET


def adr_file_operation():

    tree_adr = ET.parse('price/adrenalin.xml')
    root_adr = tree_adr.getroot()

    data_adr = {}

    for offer_adr in root_adr.findall('./item'):
        vendor_code_adr = offer_adr.find('code').text

        offer_data_adr = {}

        if offer_adr.find('stock').text == "Y":
            offer_data_adr['available'] = "В наявності"
        else:
            offer_data_adr['available'] = 'Немає в наявності'

        rrc_elem = offer_adr.find('rrc')
        if rrc_elem is not None and rrc_elem.text is not None:
            offer_data_adr['price'] = float(rrc_elem.text)

        data_adr[vendor_code_adr] = offer_data_adr

    return data_adr


# print(adr_file_operation())


