import pandas as pd
import sqlite3

from utils.logger import getlogger

logger = getlogger(__name__)


class Orders:
    def __init__(self, order_number, product_quantity, unit_price, payment_type, payment_billing_code, payment_date):
        self.order_number = order_number
        self.product_quantity = product_quantity
        self.unit_price = unit_price
        self.payment_type = payment_type
        self.payment_billing_code = payment_billing_code
        self.payment_date = payment_date


class OrdersDb:
    def __init__(self, db):
        self.db = db
        self.cursor = db.cursor

    def create_table(self):
        try:
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
            self.db.conn.commit()
            logger.info("Table created successfully")
        except Exception as e:
            logger.error("Error while creating table: %s", e)
            self.db.conn.rollback()

    def insert_order(self, orders_df):
        # define a SQL query to check for matching orders
        query = "SELECT * FROM orders WHERE OrderNumber = ?"

        logger.info("Checking for orders in Database") # pragma: no cover
        # loop over each row in the DataFrame and check for a matching order in the database
        for index, row in orders_df.iterrows():
            params = (row['OrderNumber'],)
            existing_order = pd.read_sql_query(query, self.db.conn, params=params)
            if not existing_order.empty:
                logger.warn(f"Order %s already exists in database", params) # pragma: no cover
                pass
            else:
                try:
                    self.cursor.execute('''INSERT INTO orders (OrderNumber, ProductQuantity, UnitPrice, PaymentType, 
                                        PaymentBillingCode, PaymentDate) VALUES (?, ?, ?, ?, ?, ?)''',
                                        (row['OrderNumber'], row['ProductQuantity'], row['UnitPrice'],
                                         row['PaymentType'],
                                         row['PaymentBillingCode'], row['PaymentDate']))
                    self.db.conn.commit()
                except sqlite3.Error as e:
                    logger.error("error while inserting data in order table is : %s", e) # pragma: no cover

        self.db.conn.commit()

    def close(self):
        self.db.conn.close()

    def drop_temp_table(self, table_name):
        query = 'drop table ' + table_name
        self.cursor.execute(query)
        self.db.conn.commit()


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
