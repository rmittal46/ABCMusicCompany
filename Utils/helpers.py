# Functions that are going to help ingestion, transformation & cleanup of file

import os
import pandas as pd
import yaml
from Utils.logger import getlogger

logger = getlogger(__name__)


# load properties file
def load_yaml(args):
    with open(os.path.join(getPath(), 'resource/configs/', args.properties), "r") as f:
        config = yaml.safe_load(f)
    logger.info("Loading properties file : %s", args.properties)
    return config


def read_csv(filepath):
    # Read the CSV file into a DataFrame
    logger.info("Reading the csv file started")
    file_data = pd.read_csv(os.path.join(getPath(), filepath))
    logger.info("Reading the csv file completed")
    return file_data


def getPath():
    path = os.getcwd()
    return path


def getOrderDetailKeys(db, file_data):
    # read the customer table into a pandas dataframe
    customer_df = pd.read_sql_query("SELECT  Distinct Customer_id, First_name, Last_name FROM customers", db.conn)

    # read the product table into a pandas dataframe
    product_df = pd.read_sql_query("SELECT Distinct ProductName, Product_id FROM products", db.conn)

    # read the Delivery Address table into a pandas dataframe
    address_df = pd.read_sql_query("SELECT Distinct Address_id, Address_line, DeliveryPostcode FROM delivery_addresses",
                                   db.conn)

    file_data = file_data.drop_duplicates(
        subset=['OrderNumber', 'ClientName', 'ProductName', 'DeliveryAddress', 'DeliveryPostcode']).loc[:,
                ['OrderNumber', 'First_name', 'Last_name', 'ProductName', 'DeliveryAddress', 'DeliveryPostcode']]

    merged_df = pd.merge(file_data, customer_df, on=['First_name', 'Last_name']).merge(product_df,
                                                                                       on=['ProductName']).merge(
        address_df, left_on=['DeliveryAddress', 'DeliveryPostcode'], right_on=['Address_line', 'DeliveryPostcode'])

    merged_df = merged_df[['OrderNumber', 'Customer_id', 'Address_id', 'Product_id']]

    return merged_df
