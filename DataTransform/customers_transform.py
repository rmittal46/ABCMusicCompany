import os
import pandas as pd

from Customers.customer_table import CustomersDB, CustomerLoader
from Utils.helpers import getPath

def load_customers(loader_df):

    # Connect to SQLite database
    db = CustomersDB(os.path.join(getPath(), 'resource/music_warehouse.db'))
    loader_df = CustomerLoader('/home/rahul/Documents/git/ABCMusicCompany/resource/input_data/orders_1.csv')

    unique_customers = loader_df.get_unique_customers()
    print(unique_customers)

    split_name = loader_df.split_customer_name(unique_customers)

    temp_table = 'temp_customers'
    split_name.to_sql(temp_table, db.conn, if_exists='replace', index=False)

    db.insert_customer(split_name)

    db.conn.commit()
    db.cursor