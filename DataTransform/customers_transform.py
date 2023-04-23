from DataModels.customer_table import CustomersDB, CustomerLoader
from utils.logger import getlogger

logger = getlogger(__name__)


def load_customers(db, file_data):
    # creating instance of customersDb class
    customer = CustomersDB(db)

    customer.create_table()

    # creating instance of CustomerLoader class
    loader_df = CustomerLoader(file_data)

    # get the unique customers from the csv file data
    unique_customers = loader_df.get_unique_customers()

    # defining temp table name
    temp_table = 'temp_customers'

    # Create temp table with the transformed data from csv file
    unique_customers.to_sql(temp_table, db.conn, if_exists='replace', index=False)

    # insert or merge data into main table
    customer.insert_customer(unique_customers)
    logger.info("customers inserted")

    # Drop temp table
    customer.drop_temp_table(temp_table)

    db.conn.commit()
