import pandas as pd
import sqlite3
from datetime import datetime

from utils.logger import getlogger

logger = getlogger(__name__)


class Product:
    def __init__(self, product_name, product_type, unit_price, product_quantity, currency):
        self.product_name = product_name
        self.product_type = product_type
        self.unit_price = unit_price
        self.product_quantity = product_quantity
        self.currency = currency


class ProductsDB:
    def __init__(self, db):
        self.db = db
        self.cursor = db.cursor

    def create_table(self):
        try:
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
            self.db.conn.commit()
            logger.info("Table created successfully")
        except Exception as e:
            logger.error("Error while creating table: %s", e)
            self.db.conn.rollback()

    def insert_product(self, product):

        # define a SQL query to check for matching product names
        query1 = "SELECT * FROM products WHERE ProductName = ? and CurrentFlag = 1"

        # loop over each row in the DataFrame and check for a matching customer in the database
        for index, row in product.iterrows():
            params = (row['ProductName'],)
            existing_product = pd.read_sql_query(query1, self.db.conn, params=params)
            if not existing_product.empty:
                logger.warn("Product %s already exists in database", params) # pragma: no cover
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
                    self.db.conn.commit()
                except sqlite3.Error as e:
                    logger.error("error while inserting data is : %s", e) # pragma: no cover
                self.db.conn.commit()
            else:
                # insert the new product into the database
                self.cursor.execute('''INSERT INTO products (ProductName, ProductType, UnitPrice, 
                                    ProductQuantity, Currency, CurrentFlag, EffectiveFrom, EffectiveTo) VALUES (?, ?, ?, ?, ?, 1, ?, '9999-12-31')''',
                                    (row['ProductName'], row['ProductType'], row['UnitPrice'],
                                     row['ProductQuantity'], row['Currency'], datetime.today().strftime('%Y-%m-%d')))
                self.db.conn.commit()

    def close(self):
        self.db.conn.close()

    def drop_temp_table(self, table_name):
        query = 'drop table ' + table_name
        self.cursor.execute(query)
        self.db.conn.commit()


class ProductLoader:
    def __init__(self, file_data):
        self.products_df = file_data

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
