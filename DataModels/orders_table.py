import pandas as pd
import sqlite3

from Utils.logger import getlogger

logger = getlogger(__name__)


class Orders:
    def __init__(self, order_number, product_type, unit_price, payment_type, payment_billing_code, payment_date):
        self.order_number = order_number
        self.product_type = product_type
        self.unit_price = unit_price
        self.payment_type = payment_type
        self.payment_billing_code = payment_billing_code
        self.payment_date = payment_date


class OrdersDb:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

        logger.info("Creating Order table if not exists in db")
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                OrderNumber TEXT PRIMARY KEY,
                ProductQuantity INTEGER,
                UnitPrice NUMERIC,
                PaymentType TEXT,
                PaymentBillingCode TEXT,
                PaymentDate DATE
            );
        ''')
        self.conn.commit()

    def insert_order(self, orders_df):
        # define a SQL query to check for matching orders
        query = "SELECT * FROM orders WHERE OrderNumber = ?"

        logger.info("Checking for orders in Database")
        # loop over each row in the DataFrame and check for a matching order in the database
        for index, row in orders_df.iterrows():
            params = (row['OrderNumber'],)
            existing_order = pd.read_sql_query(query, self.conn, params=params)
            if not existing_order.empty:
                logger.warn(f"Order %s already exists in database",params)
                pass
            else:
                try:
                    self.cursor.execute('''INSERT INTO orders (OrderNumber, ProductQuantity, UnitPrice, PaymentType, 
                                        PaymentBillingCode, PaymentDate) VALUES (?, ?, ?, ?, ?, ?)''',
                                        (row['OrderNumber'], row['ProductQuantity'], row['UnitPrice'], row['PaymentType'],
                                         row['PaymentBillingCode'], row['PaymentDate']))
                    self.conn.commit()
                except sqlite3.Error as e:
                    logger.error("error while inserting data in order table is : %s", e)

        self.conn.commit()


    def close(self):
        self.conn.close()


class OrdersLoader:
    def __init__(self, file_data):
        self.orders_df = file_data

    def get_unique_orders(self):
        return self.orders_df.drop_duplicates(subset=['OrderNumber'])

    def get_orders(self):
        orders = []
        for _, row in self.orders_df.iterrows():
            order = Orders(row['OrderNumber'], row['ProductQuantity'], row['UnitPrice'], row['PaymentType'],
                                         row['PaymentBillingCode'], row['PaymentDate'])
            orders.append(order)
        return orders
