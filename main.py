from processing import main_adr, main_atl, main_dasmart, main_dosp, main_kemp, main_norf, main_outfit, main_shamb, \
    main_swa, main_trp
from for_update import download_file, add_for_update, create_price_xlsx
from load_csv_to_db import processing_csv
from load_sale_file import download_file_from_s3, load_excel_to_database
from join_table_load_s3 import fetch_data_from_db, process_data, upload_to_s3

import sys
sys.path.append("..")
from config.config import link_list, SQLALCHEMY_DATABASE_URI, DB_POSTGRES_CONFIG, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY


DATABASE_URL = SQLALCHEMY_DATABASE_URI
csv_file_path = "/home/ubuntu/airflow-project/price_update_airflow/output/price_update.csv"

functions_list = [("Атлант", main_atl.atl_file_operation),
                  ("Кемпінг", main_kemp.kemping_file_operation),
                  ("Шамбала", main_shamb.shamb_file_operation),
                  ("Трамп", main_trp.trp_file_operation),
                  ("Сва", main_swa.swa_file_operation),
                  ("Доспехи", main_dosp.dosp_file_operation),
                  ("Норфін", main_norf.norf_file_operation),
                  ("Адреналін", main_adr.adr_file_operation),
                  ("Дасмарт", main_dasmart.dasmart_file_operation)]


DB_CONNECTION_STRING = SQLALCHEMY_DATABASE_URI

# Налаштування S3
BUCKET_NAME = 'for-sales-file-buck'
FILE_NAME = 'sale.xlsx'
LOCAL_FILE_PATH = 'sale.xlsx'

# Конфігурація бази даних та S3
DB_CONFIG = DB_POSTGRES_CONFIG

S3_CONFIG = {
    'bucket_name': 'for-price-update-buck',
    'aws_access_key_id': AWS_ACCESS_KEY_ID,
    'aws_secret_access_key': AWS_SECRET_ACCESS_KEY
}


def run_pipeline():
    for url_link, name_link in link_list:
        download_file(url_link, name_link)

    data_for_update = main_outfit.outfit_file_operation()

    for name_func, func in functions_list:
        result_dict = func()
        add_for_update(name_func, result_dict, data_for_update)

    create_price_xlsx(data_for_update)

    processing_csv(csv_file_path, DATABASE_URL)

    download_file_from_s3(BUCKET_NAME, FILE_NAME, LOCAL_FILE_PATH)

    load_excel_to_database(LOCAL_FILE_PATH, DB_CONNECTION_STRING)

    df = fetch_data_from_db(DB_CONFIG)

    clean_data = process_data(df)

    upload_to_s3(clean_data, S3_CONFIG)


if __name__ == "__main__":
    run_pipeline()
