import os

from DataModels.delivery_addresses_table import DeliveryAddressDB, AddressLoader
from Utils.helpers import getPath
from Utils.logger import getlogger

logger = getlogger(__name__)


def load_address(file_data):
    # Connect to SQLite database
    db = DeliveryAddressDB(os.path.join(getPath(), 'resource/music_warehouse.db'))
    loader_df = AddressLoader(file_data)

    unique_addresses = loader_df.get_unique_address()

    address_df = loader_df.getCustomer_id(unique_addresses, db)

    temp_table = 'temp_address'
    address_df.to_sql(temp_table, db.conn, if_exists='replace', index=False)

    db.insert_address(address_df)
    logger.info("Addresses inserted into the table")

    db.conn.commit()
    db.cursor.close()
