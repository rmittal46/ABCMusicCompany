import os

from DataModels.order_details_table import OrderDetailsDB, OrderDetailsLoader
from utils.helpers import getPath
from utils.logger import getlogger

logger = getlogger(__name__)


def load_order_details(db, file_data):
    # creating instance of OrderDetailsDB class
    orderDetails = OrderDetailsDB(db)

    orderDetails.create_table()

    # creating instance of OrderDetailsLoader class
    loader_df = OrderDetailsLoader(file_data)

    order_details = loader_df.get_keys(db, file_data)

    temp_table = 'temp_order_details'
    order_details.to_sql(temp_table, db.conn, if_exists='replace', index=False)

    orderDetails.insert_order_details(order_details)
    logger.info("OrderDetails inserted into the table")

    # Drop temp table
    orderDetails.drop_temp_table(temp_table)

    db.conn.commit()
    db.close()
