import os

from Orders.orders_table import OrdersDb, OrdersLoader
from Utils.helpers import getPath
from Utils.logger import getlogger

logger = getlogger(__name__)

def load_orders(loader_df):
    # Connect to SQLite database
    db = OrdersDb(os.path.join(getPath(), 'resource/music_warehouse.db'))
    loader_df = OrdersLoader('/home/rahul/Documents/git/ABCMusicCompany/resource/input_data/orders_1.csv')

    unique_orders = loader_df.get_unique_orders()

    temp_table = 'temp_orders'
    unique_orders.to_sql(temp_table, db.conn, if_exists='replace', index=False)

    db.insert_order(unique_orders)
    logger.info("Orders inserted")

    db.conn.commit()
    db.cursor.close()
