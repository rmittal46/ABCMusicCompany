import sqlite3 , os
import pandas as pd
from datetime import datetime

from Products.product_table import ProductsDB, ProductLoader
from Utils.helpers import getPath

def load_products(loader_df):

    # Connect to SQLite database
    db = ProductsDB(os.path.join(getPath(), 'resource/music_warehouse.db'))
    loader_df = ProductLoader('/home/rahul/Documents/git/ABCMusicCompany/resource/input_data/orders_1.csv')

    unique_products = loader_df.get_unique_products()
    print(unique_products)
    Product_quantity = loader_df.get_product_quantity_by_product()
    print(Product_quantity)
    products = pd.merge(unique_products, Product_quantity, on=['ProductName', 'ProductType'])

    products.to_sql('temp_products', db.conn, if_exists='replace', index=False)

    db.insert_product(products)

    db.cursor.execute('''
            INSERT INTO products
            (ProductName, ProductType, UnitPrice, ProductQuantity, currency, CurrentFlag, EffectiveFrom, EffectiveTo)
            SELECT ProductName, producttype, unitprice, productquantity, currency, 1, ?, '9999-12-31'
            FROM temp_products
            WHERE NOT EXISTS (
                SELECT 1 FROM products WHERE products.ProductName = temp_products.productname
            );
        ''', (datetime.today().strftime('%Y-%m-%d'),))
    db.conn.commit()
    db.cursor