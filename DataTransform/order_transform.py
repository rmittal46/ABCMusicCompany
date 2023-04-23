from DataModels.orders_table import OrdersDb, OrdersLoader
from utils.helpers import getPath
from utils.logger import getlogger

logger = getlogger(__name__)


def load_orders(db, file_data):
    # creating instance of OrdersDb class
    orders = OrdersDb(db)

    orders.create_table()

    # creating instance of OrdersLoader class
    loader_df = OrdersLoader(file_data)

    # get the unique orders from the csv file data
    unique_orders = loader_df.get_unique_orders()

    # defining temp table name
    temp_table = 'temp_orders'

    # Create temp table with the transformed data from csv file
    unique_orders.to_sql(temp_table, db.conn, if_exists='replace', index=False)

    orders.insert_order(unique_orders)
    logger.info("Orders inserted")

    # Drop temp table
    orders.drop_temp_table(temp_table)

    db.conn.commit()
