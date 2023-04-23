import os

from DataModels.order_details_table import OrderDetailsDB, OrderDetailsLoader
from Utils.helpers import getPath
from Utils.logger import getlogger

logger = getlogger(__name__)


def load_order_details(file_data):
    # Connect to SQLite database
    db = OrderDetailsDB(os.path.join(getPath(), 'resource/music_warehouse.db'))
    loader_df = OrderDetailsLoader(file_data)

    order_details = loader_df.get_keys(db, file_data)

    temp_table = 'temp_order_details'
    order_details.to_sql(temp_table, db.conn, if_exists='replace', index=False)

    db.insert_order_details(order_details)
    logger.info("OrderDetails inserted into the table")

    # Drop temp table
    db.drop_table(temp_table)

    db.conn.commit()
    db.close()
