import pandas as pd

from DataModels.product_table import ProductsDB, ProductLoader
from utils.logger import getlogger

logger = getlogger(__name__)


def load_products(db, file_data):
    # creating instance of ProductsDb class
    product = ProductsDB(db)

    product.create_table()

    # creating instance of ProductLoader class
    loader_df = ProductLoader(file_data)

    # get the unique products from the csv file data
    unique_products = loader_df.get_unique_products()

    # get the product quantity per product from csv file
    Product_quantity = loader_df.get_product_quantity_by_product()

    products = pd.merge(unique_products, Product_quantity, on=['ProductName', 'ProductType'])

    # defining temp table name
    temp_table = 'temp_products'

    # Create temp table with the transformed data from csv file
    products.to_sql(temp_table, db.conn, if_exists='replace', index=False)

    # insert or  merge data into main table
    product.insert_product(products)
    logger.info("Products inserted")

    # Drop temp table
    product.drop_temp_table(temp_table)

    db.conn.commit()
