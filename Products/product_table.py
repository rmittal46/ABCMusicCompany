import pandas as pd
import sqlite3
from datetime import datetime

from Utils.logger import getlogger

logger = getlogger(__name__)


class Product:
    def __init__(self, product_name, product_type, unit_price, quantity, currency):
        self.product_name = product_name
        self.product_type = product_type
        self.unit_price = unit_price
        self.quantity = quantity
        self.currency = currency


class ProductsDB:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

        logger.info("Creating Product table if not exists in db")
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                Product_id INTEGER PRIMARY KEY AUTOINCREMENT,
                ProductName TEXT,
                ProductType TEXT,
                ProductQuantity INTEGER,
                UnitPrice REAL,
                Currency TEXT,
                EffectiveFrom DATETIME DEFAULT CURRENT_TIMESTAMP,
                EffectiveTo DATETIME DEFAULT '9999-12-31 23:59:59.999',
                CurrentFlag INTEGER DEFAULT 1
            );
        ''')
        self.conn.commit()

    def insert_product(self, product):

        # define a SQL query to check for matching product names
        query1 = "SELECT * FROM products WHERE ProductName = ? "

        # loop over each row in the DataFrame and check for a matching customer in the database
        for index, row in product.iterrows():
            params = (row['ProductName'],)
            existing_product = pd.read_sql_query(query1, self.conn, params=params)
            if not existing_product.empty:
                logger.warn("Product %s already exists in database", params)
                existing_products = existing_product.iloc[0]
                try:
                    self.cursor.execute('''
                                UPDATE products
                                SET CurrentFlag = 0, EffectiveTo = ?
                                WHERE Product_id = ?;
                            ''', (datetime.today().strftime('%Y-%m-%d'), existing_products['Product_id']))

                    self.cursor.execute('''INSERT INTO products (ProductName, ProductType, UnitPrice, 
                    ProductQuantity, Currency, CurrentFlag, EffectiveFrom, EffectiveTo) VALUES (?, ?, ?, ?, ?, 1, ?, '9999-12-31')''',
                                        (row['ProductName'], row['ProductType'],row['UnitPrice'],
                                         row['ProductQuantity'],row['Currency'], datetime.today().strftime('%Y-%m-%d')))
                    self.conn.commit()
                except sqlite3.Error as e:
                    logger.error("error while inserting data is : %s", e)
                self.conn.commit()
            else:
                # insert the new product into the database
                self.cursor.execute('''INSERT INTO products (ProductName, ProductType, UnitPrice, 
                                    ProductQuantity, Currency, CurrentFlag, EffectiveFrom, EffectiveTo) VALUES (?, ?, ?, ?, ?, 1, ?, '9999-12-31')''',
                                    (row['ProductName'], row['ProductType'], row['UnitPrice'],
                                     row['ProductQuantity'], row['Currency'], datetime.today().strftime('%Y-%m-%d')))
                self.conn.commit()

    def close(self):
        self.conn.close()


class ProductLoader:
    def __init__(self, file_path):
        self.products_df = pd.read_csv(file_path)

    def get_unique_products(self):
        return self.products_df.drop_duplicates(subset=['ProductName', 'ProductType', 'UnitPrice', 'Currency']).loc[:,
               ['ProductName', 'ProductType', 'UnitPrice', 'Currency']]

    def get_product_quantity_by_product(self):
        return self.products_df.groupby(['ProductName', 'ProductType', ]).agg({'ProductQuantity': 'sum'}).reset_index()

    def get_products(self):
        products = []
        for _, row in self.products_df.iterrows():
            product = Product(row['ProductName'], row['ProductType'], row['UnitPrice'], row['ProductQuantity'],
                              row['Currency'])
            products.append(product)
        return products
