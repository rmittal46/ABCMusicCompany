import pandas as pd
import sqlite3

from utils.logger import getlogger

logger = getlogger(__name__)


class Customer:
    def __init__(self, first_name, last_name, email, phone, is_active):
        self.first_name = first_name
        self.last_name = last_name
        self.email = email
        self.phone = phone
        self.is_active = is_active


class CustomersDB:
    def __init__(self, db):
        self.db = db
        self.cursor = db.cursor

    def create_table(self):
        try:
            self.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS customers (
                        Customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        First_name TEXT,
                        Last_name Text,
                        Email TEXT,
                        Phone TEXT,
                        IsActive INTEGER DEFAULT 1
                    );
                ''')
            self.db.conn.commit()
            logger.info("Table created successfully")
        except Exception as e:
            logger.error("Error while creating table: %s", e)
            self.db.conn.rollback()

    def insert_customer(self, customer_df):
        # define a SQL query to check for matching customer names
        query = "SELECT * FROM customers WHERE First_name = ? AND Last_name = ?"

        customer_df = customer_df[['First_name', 'Last_name']]

        # loop over each row in the DataFrame and check for a matching customer in the database
        for index, row in customer_df.iterrows():
            params = (row['First_name'], row['Last_name'])
            existing_customer = pd.read_sql_query(query, self.db.conn, params=params)
            if not existing_customer.empty:
                logger.warn("Customer %s already exists in database", params)  # pragma: no cover
                pass
            else:
                try:
                    self.cursor.execute("INSERT INTO customers (First_name, Last_name, IsActive) VALUES (?, ?, ?)",
                                           (row['First_name'], row['Last_name'], 1))
                    self.db.conn.commit()
                except sqlite3.Error as e:
                    logger.error("error while inserting customers data is : %s", e)  # pragma: no cover

        self.db.conn.commit()

    def close(self):
        self.db.conn.close()

    def drop_temp_table(self, table_name):
        query = 'drop table ' + table_name
        self.cursor.execute(query)
        self.db.conn.commit()


class CustomerLoader:
    def __init__(self, file_data):
        self.customers_df = file_data

    def get_unique_customers(self):
        return self.customers_df.drop_duplicates(subset=['ClientName']).loc[:, ['First_name', 'Last_name']].reset_index(
            drop=True)

    def get_customers(self):
        customers = []
        for _, row in self.customers_df.iterrows():
            customer = Customer(row['First_name'], row['Last_name'], row['Email'], row['Phone'], row['IsActive'])
            customers.append(customer)
        return customers
