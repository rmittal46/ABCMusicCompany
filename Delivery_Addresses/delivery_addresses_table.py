import pandas as pd
import sqlite3
from datetime import datetime

from Utils.logger import getlogger

logger = getlogger(__name__)


class DeliveryAddress:
    def __init__(self, address_line, delivery_city, delivery_postcode, delivery_country, delivery_contact_number):
        self.address_line = address_line
        self.delivery_city = delivery_city
        self.delivery_postcode = delivery_postcode
        self.delivery_country = delivery_country
        self.delivery_contact_number = delivery_contact_number


class DeliveryAddressDB:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

        logger.info("Creating Address table if not exists in db")
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
                CurrentFlag INTEGER DEFAULT 1,
                FOREIGN KEY (Customer_id) REFERENCES customers (Customer_id)
            );
        ''')
        self.conn.commit()

    def insert_address(self, address):

        # define a SQL query to check for matching address for a particular customer
        query = "SELECT * FROM delivery_addresses WHERE Customer_id = ? and DeliveryPostcode = ?"

        # loop over each row in the DataFrame and check for a matching customer in the database
        for index, row in address.iterrows():
            params = (row['Customer_id'],row['DeliveryPostcode'])
            existing_address = pd.read_sql_query(query, self.conn, params=params)
            if not existing_address.empty:
                logger.warn(f"Address %s already exists in database", params)
                existing_addresses = existing_address.iloc[0]
                try:
                    self.cursor.execute('''
                                UPDATE delivery_addresses
                                SET CurrentFlag = 0, EffectiveTo = ?
                                WHERE Customer_id = ? and DeliveryPostcode = ?;
                            ''', (datetime.today().strftime('%Y-%m-%d'), existing_addresses['Customer_id'],
                                  existing_addresses['DeliveryPostcode']))

                    self.cursor.execute('''INSERT INTO delivery_addresses (Customer_id, Address_line, DeliveryCity, 
                    DeliveryPostcode, DeliveryCountry, DeliveryContactNumber, CurrentFlag, EffectiveFrom, EffectiveTo) 
                    VALUES (?, ?, ?, ?, ?, ?, 1, ?, '9999-12-31')''',
                                        (row['Customer_id'], row['DeliveryAddress'],row['DeliveryCity'],
                                         row['DeliveryPostcode'],row['DeliveryCountry'],row['DeliveryContactNumber'],
                                         datetime.today().strftime('%Y-%m-%d')))
                    self.conn.commit()
                except sqlite3.Error as e:
                    logger.error("error while inserting data is : %s", e)
                self.conn.commit()
            else:
                try:
                    # insert the new product into the database
                    self.cursor.execute('''INSERT INTO delivery_addresses (Customer_id, Address_line, DeliveryCity, 
                                    DeliveryPostcode, DeliveryCountry, DeliveryContactNumber, CurrentFlag, EffectiveFrom, EffectiveTo) 
                                    VALUES (?, ?, ?, ?, ?, ?, 1, ?, '9999-12-31')''',
                                    (row['Customer_id'], row['DeliveryAddress'], row['DeliveryCity'],
                                     row['DeliveryPostcode'], row['DeliveryCountry'], row['DeliveryContactNumber'],
                                     datetime.today().strftime('%Y-%m-%d')))
                except sqlite3.Error as e:
                    logger.error("error while inserting data is : %s", e)
                self.conn.commit()

    def close(self):
        self.conn.close()


class AddressLoader:
    def __init__(self, file_path):
        self.address_df = pd.read_csv(file_path)

    def getCustomer_id(self, dataframe, db):
        # read the customer table into a pandas dataframe
        customer_df = pd.read_sql_query("SELECT Customer_id, First_name, Last_name FROM customers", db.conn)

        # get the first_name and last_name
        dataframe[['First_name', 'Last_name']] = self.address_df['ClientName'].str.split(n=1, expand=True)

        # join the customer and orders tables on the customer name column
        merged_df = pd.merge(customer_df, dataframe, on=['First_name','Last_name'])

        # drop column B from the dataframe
        merged_df = merged_df.drop(columns=['First_name','Last_name', 'ClientName'], axis=1)

        # # extract the customer ID column from the merged dataframe
        # customer_id = merged_df['Customer_id']

        return merged_df

    def get_unique_address(self):
        return self.address_df.drop_duplicates(subset=['DeliveryAddress', 'DeliveryCity', 'DeliveryPostcode', 'DeliveryCountry']).loc[:,['ClientName', 'DeliveryAddress', 'DeliveryCity', 'DeliveryPostcode', 'DeliveryCountry', 'DeliveryContactNumber']]

    def get_address(self):
        addresses = []
        for _, row in self.products_df.iterrows():
            address = DeliveryAddress(row['DeliveryAddress'], row['DeliveryCity'],
                                     row['DeliveryPostcode'], row['DeliveryCountry'], row['DeliveryContactNumber'])
            address.append(addresses)
        return address