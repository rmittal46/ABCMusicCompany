from DataTransform.address_transform import load_address
from DataTransform.customers_transform import load_customers
from DataTransform.dataCleaning import clean_data
from DataTransform.order_transform import load_orders
from DataTransform.products_transform import load_products
from Utils.argument_parser import getparser
from Utils.constants import *
from Utils.helpers import *
from Utils.logger import getlogger


logger = getlogger(__name__)

def run():

    args = getparser()

    # To read properties file
    properties = load_yaml(args)

    # Defining Global variables
    tableName = properties[table_name]
    fileName = properties[file_name]
    filePath = properties[file_path] + fileName
    primaryKey = properties[primary_key]

    # To load the file to pandas dataframe & perform operations on it
    logger.info("File Loading in pandas Dataframe starts")
    data_df = load_file(filePath)
    logger.info("File Loading ends")

    # To perform the data cleaning
    logger.info("File Cleanup Starts")
    clean_df = clean_data(data_df)
    logger.info("File Cleanup Completed")

    # Ingest into customers table
    load_customers(clean_df)

    # ingest into products table
    load_products(clean_df)

    # ingest into Address Table
    load_address(clean_df)

    # ingest into Orders Table
    load_orders(clean_df)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run()
