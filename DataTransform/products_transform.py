import os
import pandas as pd
from datetime import datetime

from Products.product_table import ProductsDB, ProductLoader
from Utils.helpers import getPath
from Utils.logger import getlogger

logger = getlogger(__name__)


def load_products(loader_df):
    # Connect to SQLite database
    db = ProductsDB(os.path.join(getPath(), 'resource/music_warehouse.db'))
    loader_df = ProductLoader('/home/rahul/Documents/git/ABCMusicCompany/resource/input_data/orders_1.csv')

    unique_products = loader_df.get_unique_products()

    Product_quantity = loader_df.get_product_quantity_by_product()

    products = pd.merge(unique_products, Product_quantity, on=['ProductName', 'ProductType'])

    products.to_sql('temp_products', db.conn, if_exists='replace', index=False)

    db.insert_product(products)
    logger.info("Products inserted")

    db.conn.commit()
    db.cursor.close()
