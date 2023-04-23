from DataModels.Database_details import Database
from DataTransform.address_transform import load_address
from DataTransform.customers_transform import load_customers
from DataTransform.dataCleaning import clean_data
from DataTransform.orderDetails_transform import load_order_details
from DataTransform.order_transform import load_orders
from DataTransform.products_transform import load_products
from utils.argument_parser import getparser
from utils.constants import *
from utils.helpers import *
from utils.logger import getlogger

logger = getlogger(__name__)


def run():
    # feature To provide arguments and run application basis on config files
    args = getparser()

    # To read properties file
    properties = load_yaml(args)

    # Defining Global variables
    input_file_name = properties[file_name]
    filePath = properties[file_path] + input_file_name
    database = properties[database_path] + properties[database_name]

    # To load the file to pandas dataframe & perform operations on it
    logger.info("File Loading in pandas Dataframe starts")
    file_data = read_csv(filePath)
    logger.info("File Loading ends")

    # To perform the data cleaning
    logger.info("Data Cleanup Starts")
    clean_df = clean_data(file_data)
    logger.info("Data Cleanup Completed")

    # Connect to SQLite database
    db = Database(os.path.join(getPath(), database))

    # Ingest into customers table
    logger.info("Starting Customer table Loading")
    load_customers(db, clean_df)

    # ingest into products table
    logger.info("Starting Products table Loading")
    load_products(db, clean_df)

    # ingest into Address Table
    logger.info("Starting Address table Loading")
    load_address(db, clean_df)

    # ingest into Orders Table
    logger.info("Starting Orders table Loading")
    load_orders(db, clean_df)

    # ingest into Orders Table
    logger.info("Starting Order_details table Loading")
    load_order_details(db, clean_df)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run()
