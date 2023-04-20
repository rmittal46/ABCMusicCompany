import os

from DataModels.customer_table import CustomersDB, CustomerLoader
from Utils.helpers import getPath
from Utils.logger import getlogger

logger = getlogger(__name__)

def load_customers(file_data):

    # Connect to SQLite database
    db = CustomersDB(os.path.join(getPath(), 'resource/music_warehouse.db'))
    loader_df = CustomerLoader(file_data)

    unique_customers = loader_df.get_unique_customers()

    split_name = loader_df.split_customer_name(unique_customers)

    temp_table = 'temp_customers'
    split_name.to_sql(temp_table, db.conn, if_exists='replace', index=False)

    db.insert_customer(split_name)
    logger.info("customers inserted")

    db.conn.commit()
    db.cursor.close()
