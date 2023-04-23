import pandas as pd

from DataModels.Database_details import Database
from DataModels.order_details_table import OrderDetailsDB, OrderDetailsLoader
from pandas.testing import assert_frame_equal


def setup_env():
    # prepare a test database with some initial data
    db_name = ':memory:'

    # create a Database instance to initiate db connection and cursor
    db = Database(db_name)
    return db


def test_insert_orderDetails():
    db = setup_env()

    # create a ordersDB object and call the insert_order method with some new orders
    orderDetails = OrderDetailsDB(db)

    orderDetails.create_table()

    db.cursor.execute('''INSERT INTO order_details (OrderNumber, Customer_id, Product_id, Address_id) 
                                           VALUES ('P00060504-1', 1, 1, 1)
                    ''')

    df = pd.DataFrame({
        'OrderNumber': ['P00060504-3'],
        'Customer_id': [1],
        'Product_id': [2],
        'Address_id': [1]
    })
    orderDetails.insert_order_details(df)

    params = (df['OrderNumber'], df['Customer_id'], df['Product_id'], df['Address_id'])

    # check that the new customers were inserted correctly into the database
    result = pd.read_sql_query(''' SELECT * FROM order_details ''', db.conn)

    # assert that the data was inserted correctly
    assert result.shape[0] == 2
    assert result['OrderNumber'].values[0] == 'P00060504-1'
    assert result['Customer_id'].values[0] == 1
    assert result['Product_id'].values[0] == 1
    assert result['Address_id'].values[0] == 1

    orderDetails.close()


def test_unique_orderDetails():
    df = pd.DataFrame({
        'OrderNumber': ['P00060504-3', 'P00060504-3'],
        'Customer_id': [1, 1],
        'Product_id': [2, 2],
        'Address_id': [1, 1]
    })

    loader_df = OrderDetailsLoader(df)

    actual_unique_orders = loader_df.get_unique_order_detail()

    expected_unique_orders = pd.DataFrame({
        'OrderNumber': ['P00060504-3'],
        'Customer_id': [1],
        'Product_id': [2],
        'Address_id': [1]
    })

    assert_frame_equal(expected_unique_orders, actual_unique_orders)


def test_get_orderDetails():
    # Create a test dataframe with some data
    test_df = pd.DataFrame({
        'OrderNumber': ['P00060504-3'],
        'Customer_id': [1],
        'Product_id': [2],
        'Address_id': [1]
    })

    # Create an instance of MyClass using the test dataframe
    loader_df = OrderDetailsLoader(test_df)

    # Call the get_customers method and check the results
    orderDetails = loader_df.get_order_details()
    assert isinstance(orderDetails, list)
    assert len(orderDetails) == 1
    for i, row in test_df.iterrows():
        assert orderDetails[i].order_number == row['OrderNumber']
        assert orderDetails[i].customer_id == row['Customer_id']
        assert orderDetails[i].product_id == row['Product_id']
        assert orderDetails[i].address_id == row['Address_id']

#
# def test_getKeys():
#     # prepare a test database with some initial data
#     db_name = ':memory:'
#
#     # create a ordersDB object and call the insert_order_details method with some new orders
#     db = OrderDetailsDB(db_name)
#
#     # Create a test dataframe with some data
#     test_df = pd.DataFrame({
#         'OrderNumber': ['P00060504-3'],
#         'Customer_id': [1],
#         'Product_id': [2],
#         'Address_id': [1]
#     })
#
#     # Create an instance of MyClass using the test dataframe
#     loader_df = OrderDetailsLoader(test_df)
#
#     order_details = loader_df.get_keys(db, test_df)
