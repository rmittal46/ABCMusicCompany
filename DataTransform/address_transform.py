from DataModels.delivery_addresses_table import DeliveryAddressDB, AddressLoader
from utils.logger import getlogger

logger = getlogger(__name__)


def load_address(db, file_data):
    # creating instance of ProductsDb class
    address = DeliveryAddressDB(db)

    address.create_table()

    # creating instance of AddressLoader class
    loader_df = AddressLoader(file_data)

    # get the unique addresses from the csv file data
    unique_addresses = loader_df.get_unique_address()

    # get the customer_id from customer table by performing join
    address_df = loader_df.getCustomer_id(unique_addresses, db)

    # defining temp table name
    temp_table = 'temp_address'

    # Create temp table with the transformed data from csv file
    address_df.to_sql(temp_table, db.conn, if_exists='replace', index=False)

    # insert or  merge data into main table
    address.insert_address(address_df)
    logger.info("Addresses inserted into the table")

    # Drop temp table
    address.drop_temp_table(temp_table)

    db.conn.commit()
