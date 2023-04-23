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


def dropTable(tablename):
    drop_table_sql = 'drop table ' + tablename
    logger.info("%s ", drop_table_sql)
    return drop_table_sql

def getOrderDetailKeys(db, file_data):

    # read the customer table into a pandas dataframe
    customer_df = pd.read_sql_query("SELECT  Distinct Customer_id, First_name, Last_name FROM customers", db.conn)

    # read the product table into a pandas dataframe
    product_df = pd.read_sql_query("SELECT Distinct ProductName, Product_id FROM products", db.conn)

    # read the Delivery Address table into a pandas dataframe
    address_df = pd.read_sql_query("SELECT Distinct Address_id, Address_line, DeliveryPostcode FROM delivery_addresses", db.conn)

    file_data = file_data.drop_duplicates(subset=['OrderNumber','ClientName','ProductName','DeliveryAddress', 'DeliveryPostcode']).loc[:,['OrderNumber','First_name','Last_name','ProductName','DeliveryAddress','DeliveryPostcode']]

    merged_df = pd.merge(file_data, customer_df,on=['First_name', 'Last_name']).merge(product_df,on=['ProductName']).merge(address_df,left_on=['DeliveryAddress','DeliveryPostcode'],right_on=['Address_line','DeliveryPostcode'])

    merged_df = merged_df[['OrderNumber','Customer_id','Address_id','Product_id']]

    return merged_df


def getCustomerId(db, file_data):

    # read the customer table into a pandas dataframe
    customer_df = pd.read_sql_query("SELECT Customer_id, First_name, Last_name FROM customers", db.conn)

    file_data = file_data.drop_duplicates(['ClientName'])

    client_name_df = file_data[["ClientName"]].drop_duplicates()

    # get the first_name and last_name
    file_data[['First_name', 'Last_name']] = client_name_df['ClientName'].str.split(n=1, expand=True)

    # join the customer and orders tables on the customer name column
    merged_df = pd.merge(customer_df, file_data, on=['First_name', 'Last_name'])

    # drop irrelevant from the dataframe
    merged_df = merged_df[['Customer_id']]

    return merged_df

def getProductId(db, file_data):

    # read the product table into a pandas dataframe
    product_df = pd.read_sql_query("SELECT ProductName, Product_id FROM products", db.conn)

    file_data = file_data.drop_duplicates(['ProductName'])

    product_name_df = file_data[["ProductName"]].drop_duplicates()

    # join the customer and orders tables on the customer name column
    merged_df = pd.merge(product_df, product_name_df, on=['ProductName'])

    # drop irrelevant from the dataframe
    merged_df = merged_df[['Product_id']]

    return merged_df

def getAddressId(db, file_data):

    # read the Delivery Address table into a pandas dataframe
    address_df = pd.read_sql_query("SELECT Address_id, Address_line, DeliveryPostcode FROM delivery_addresses", db.conn)

    file_data = file_data.drop_duplicates(['DeliveryAddress','DeliveryPostcode'])

    address_line_df = file_data[["DeliveryAddress","DeliveryPostcode"]].drop_duplicates()

    # join the customer and orders tables on the customer name column
    merged_df = pd.merge(address_df, address_line_df, left_on=['Address_line','DeliveryPostcode'],right_on=['DeliveryAddress','DeliveryPostcode'])

    # drop irrelevant from the dataframe
    merged_df = merged_df[['Address_id']]

    return merged_df
