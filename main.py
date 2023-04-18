from DataTransform.customers_transform import customers_transform
from DataTransform.orderDetails_transform import orderDetails_transform
from DataTransform.order_transform import orders_transform
from DataTransform.dataCleaning import clean_data
from DataTransform.products_transform import products_transform
from Utils.constants import *
from Utils.helpers import *
from Utils.logger import getlogger

logger = getlogger(__name__)

def run():

    # To read properties file
    properties = load_yaml()

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

    # To perform the business Transformation
    logger.info("Transformation on dataframe starts")
    if tableName == 'orders':
        transformed_data = orders_transform(clean_df)
    elif tableName == 'customers':
        transformed_data = customers_transform(clean_df)
    elif tableName == 'products':
        transformed_data = products_transform(clean_df)
    elif tableName == 'orderDetails':
        transformed_data = orderDetails_transform(clean_df)
    else:
        logger.error("Target tablename doesn't match")
        exit()

    logger.info("Transformation on dataframe ends")

    # To ingest data into target table
    logger.info("Ingestion to Db Starts")
    ingestToDb(transformed_data, tableName, primaryKey)
    logger.info("Ingestion Completed")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run()
