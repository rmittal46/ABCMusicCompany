import os
import pandas as pd

from DataModels.product_table import ProductsDB, ProductLoader
from Utils.helpers import getPath
from Utils.logger import getlogger

logger = getlogger(__name__)


def load_products(file_data):
    # Connect to SQLite database
    db = ProductsDB(os.path.join(getPath(), 'resource/music_warehouse.db'))
    loader_df = ProductLoader(file_data)

    unique_products = loader_df.get_unique_products()

    Product_quantity = loader_df.get_product_quantity_by_product()

    products = pd.merge(unique_products, Product_quantity, on=['ProductName', 'ProductType'])

    temp_table = 'temp_products'

    products.to_sql(temp_table, db.conn, if_exists='replace', index=False)

    db.insert_product(products)
    logger.info("Products inserted")

    # Drop temp table
    db.drop_table(temp_table)

    db.conn.commit()
    db.close()
