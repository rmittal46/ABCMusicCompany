import pandas as pd
import sqlite3

from Utils.logger import getlogger

logger = getlogger(__name__)


class Customer:
    def __init__(self, first_name, last_name, email, phone):
        self.first_name = first_name
        self.last_name = last_name
        self.email = email
        self.phone = phone


class CustomersDB:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

        logger.info("Creating customers table if not exists in db")
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
        self.conn.commit()

    def insert_customer(self, customer_df):
        # define a SQL query to check for matching customer names
        query = "SELECT * FROM customers WHERE First_name = ? AND Last_name = ?"

        # loop over each row in the DataFrame and check for a matching customer in the database
        for index, row in customer_df.iterrows():
            params = (row['First_name'], row['Last_name'])
            existing_customer = pd.read_sql_query(query, self.conn, params=params)
            if not existing_customer.empty:
                logger.warn("Customer %s already exists in database", params)
                pass
            else:
                try:
                    self.cursor.execute("INSERT INTO customers (First_name, Last_name, IsActive) VALUES (?, ?, ?)",
                                        (row['First_name'], row['Last_name'], 1))
                    self.conn.commit()
                except sqlite3.Error as e:
                    logger.error("error while inserting customers data is : %s", e)

        self.conn.commit()

    def close(self):
        self.conn.close()


class CustomerLoader:
    def __init__(self, file_data):
        self.customers_df = file_data

    def split_customer_name(self, dataframe):
        dataframe[['First_name', 'Last_name']] = self.customers_df['ClientName'].str.split(n=1, expand=True)
        return dataframe.loc[:, ['First_name', 'Last_name']]

    def get_unique_customers(self):
        self.customers_df['ClientName'] = self.customers_df['ClientName'].str.replace('[^a-zA-Z0-9 ]+', '', regex=True)
        return self.customers_df.drop_duplicates(subset=['ClientName']).loc[:, ['ClientName']]

    def get_customers(self):
        customers = []
        for _, row in self.customers_df.iterrows():
            customer = Customer(row['First_name'], row['Last_name'], row['Email'], row['Phone'], row['IsActive'])
            customers.append(customer)
        return customers
