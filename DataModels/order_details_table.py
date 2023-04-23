import pandas as pd
import sqlite3

from utils.helpers import getOrderDetailKeys
from utils.logger import getlogger

logger = getlogger(__name__)


class OrderDetails:
    def __init__(self, order_number, customer_id, product_id, address_id):
        self.order_number = order_number
        self.customer_id = customer_id
        self.product_id = product_id
        self.address_id = address_id


class OrderDetailsDB:
    def __init__(self, db):
        self.db = db
        self.cursor = db.cursor

    def create_table(self):
        try:
            self.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS order_details (
                        OrderNumber TEXT,
                        Customer_id INTEGER,
                        Product_id INTEGER,
                        Address_id INTEGER,
                        PRIMARY KEY (OrderNumber, Customer_id, Product_id, Address_id),
                        FOREIGN KEY (OrderNumber) REFERENCES orders (OrderNumber),
                        FOREIGN KEY (Product_id) REFERENCES products (Product_id),
                        FOREIGN KEY (Customer_id) REFERENCES customers (Customer_id),
                        FOREIGN KEY (Address_id) REFERENCES delivery_address (Address_id)
                    );
                ''')
            self.db.conn.commit()
            logger.info("Table created successfully")
        except Exception as e:
            logger.error("Error while creating table: %s", e)
            self.db.conn.rollback()

    def insert_order_details(self, order_detail_df):
        # define a SQL query to check for matching customer names
        query = '''SELECT * FROM order_details 
                   WHERE     OrderNumber = ? 
                         and Customer_id = ?
                         and Product_id  = ? 
                         and Address_id  = ?
                   '''

        # loop over each row in the DataFrame and check for a matching customer in the database
        for index, row in order_detail_df.iterrows():
            params = (row['OrderNumber'], row['Customer_id'], row['Product_id'], row['Address_id'])
            existing_order_detail = pd.read_sql_query(query, self.db.conn, params=params)
            if not existing_order_detail.empty:
                logger.warn(f"Order_details %s already exists in database", params)
                pass
            else:
                try:
                    self.cursor.execute('''INSERT INTO order_details (OrderNumber, Customer_id, Product_id, Address_id) 
                                           VALUES (?, ?, ?, ?)''',
                                        (row['OrderNumber'], row['Customer_id'], row['Product_id'], row['Address_id']))
                    self.db.conn.commit()
                except sqlite3.Error as e:
                    logger.error("error while inserting order_details data is : %s", e)

        self.db.conn.commit()

    def close(self):
        self.db.conn.close()

    def drop_temp_table(self, table_name):
        query = 'drop table ' + table_name
        self.cursor.execute(query)
        self.db.conn.commit()


class OrderDetailsLoader:
    def __init__(self, file_data):
        self.orderdetail_df = file_data

    def get_keys(self, db, file_data):
        order_detail_keys = getOrderDetailKeys(db, file_data)
        return order_detail_keys

    def get_unique_order_detail(self):
        return self.orderdetail_df.drop_duplicates(subset=['OrderNumber','Customer_id','Product_id','Address_id'])

    def get_order_details(self):
        order_details = []
        for _, row in self.orderdetail_df.iterrows():
            order_detail = OrderDetails(row['OrderNumber'], row['Customer_id'], row['Product_id'], row['Address_id'])
            order_details.append(order_detail)
        return order_details
