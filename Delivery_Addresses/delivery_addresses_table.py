import pandas as pd
import sqlite3
from datetime import datetime

class Delivery_Addresses:
    def __init__(self, address_line, delivery_city, delivery_postcode, delivery_country, delivery_contact_number):
        self.address_line = address_line
        self.delivery_city = delivery_city
        self.delivery_postcode = delivery_postcode
        self.delivery_country = delivery_country
        self.delivery_contact_number = delivery_contact_number


class Delivery_AddressesDB:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS delivery_addresses (
                Address_id INTEGER PRIMARY KEY AUTOINCREMENT,
                Customer_id INTEGER,
                Address_line TEXT,
                DeliveryCity TEXT,
                DeliveryPostcode TEXT,
                DeliveryCountry TEXT,
                DeliveryContactNumber TEXT,
                EffectiveFrom DATETIME DEFAULT CURRENT_TIMESTAMP,
                EffectiveTo DATETIME DEFAULT '9999-12-31 23:59:59.999',
                CurrentFlag BOOLEAN DEFAULT 1,
                FOREIGN KEY (Customer_id) REFERENCES customers (Customer_id)
            );
        ''')
        self.conn.commit()

    def insert_address(self, d_address):
        existing_addresses = pd.read_sql_query("SELECT * FROM delivery_addresses WHERE Address_line = ? and DeliveryPostcode = ?", self.conn, params=[d_address.address_line, d_address.delivery_postcode])
        if len(existing_addresses) > 0:
            # Update existing product with SCD Type 2 logic
            existing_addresses = existing_addresses.iloc[0]
            self.cursor.execute('''
                UPDATE delivery_addresses
                SET CurrentFlag = 0, EffectiveTo = ?
                WHERE Customer_id = ?;
            ''', (datetime.today().strftime('%Y-%m-%d'), existing_addresses['Product_id']))
            self.conn.commit()
            self.insert_new_product(d_address)
        else:
            self.insert_new_product(d_address)

    def insert_new_product(self, product):
        self.cursor.execute('''
            INSERT INTO products
            (ProductName, ProductType, Price, ProductQuantity, Currency, CurrentFlag, EffectiveFrom, EffectiveTo)
            VALUES (?, ?, ?, ?, ?, 1, ?, '9999-12-31');
        ''', (product.name, product.type, product.unit_price, product.quantity, product.currency, datetime.today().strftime('%Y-%m-%d')))
        self.conn.commit()

    def close(self):
        self.conn.close()


class ProductLoader:
    def __init__(self, file_path):
        self.products_df = pd.read_csv(file_path)

    def get_unique_products(self):
        return self.products_df.drop_duplicates(subset=['ProductName', 'ProductType', 'UnitPrice', 'Currency']).loc[:,['ProductName', 'ProductType', 'UnitPrice', 'Currency']]

    def get_product_quantity_by_product(self):
        return self.products_df.groupby(['ProductName', 'ProductType', ]).agg({'ProductQuantity': 'sum'}).reset_index()

    def get_products(self):
        products = []
        for _, row in self.products_df.iterrows():
            product = Product(row['ProductName'], row['ProductType'], row['UnitPrice'], row['ProductQuantity'], row['Currency'])
            products.append(product)
        return products