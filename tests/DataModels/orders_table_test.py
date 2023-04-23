import pandas as pd
import pytest

from DataModels.Database_details import Database
from DataModels.orders_table import OrdersDb, OrdersLoader
from pandas.testing import assert_frame_equal


def setup_env():
    # prepare a test database with some initial data
    db_name = ':memory:'

    # create a Database instance to initiate db connection and cursor
    db = Database(db_name)
    return db


def test_insert_orders():
    db = setup_env()
    # create a ordersDB object and call the insert_order method with some new orders
    orders = OrdersDb(db)
    orders.create_table()

    db.cursor.execute('''INSERT INTO orders (OrderNumber, ProductQuantity, UnitPrice, PaymentType, 
                                        PaymentBillingCode, PaymentDate) 
                                        values ('PO0060504-1','3',4700,'Debit','PO0060504-20210321','21/03/2021')
                    ''')

    df = pd.DataFrame({
        'OrderNumber': ['Po0024697-1'],
        'ProductQuantity': [2],
        'UnitPrice': [2000.0],
        'PaymentType': ['CREDIT'],
        'PaymentBillingCode': ['Po0024697-20210127'],
        'PaymentDate': ['27/01/2021']
    })
    orders.insert_order(df)

    # check that the new customers were inserted correctly into the database
    result = pd.read_sql_query('SELECT * FROM orders', db.conn)

    # assert that the data was inserted correctly
    assert result.shape[0] == 2
    assert result['OrderNumber'].values[0] == 'PO0060504-1'
    assert result['ProductQuantity'].values[0] == 3
    assert result['UnitPrice'].values[0] == 4700.0
    assert result['PaymentType'].values[0] == 'Debit'
    assert result['PaymentBillingCode'].values[0] == 'PO0060504-20210321'
    assert result['PaymentDate'].values[0] == '21/03/2021'

    db.close()


def test_unique_orders():
    df = pd.DataFrame({
        'OrderNumber': ['Po0024697-1', 'Po0024697-1'],
        'ProductQuantity': [2, 1],
        'UnitPrice': [2000.0, 3000.0],
        'PaymentType': ['CREDIT', 'CREDIT'],
        'PaymentBillingCode': ['Po0024697-20210127', 'Po0024697-20210127'],
        'PaymentDate': ['27/01/2021', '27/01/2021']
    })

    loader_df = OrdersLoader(df)

    actual_unique_orders = loader_df.get_unique_orders()

    expected_unique_orders = pd.DataFrame({
        'OrderNumber': ['Po0024697-1'],
        'ProductQuantity': [2],
        'UnitPrice': [2000.0],
        'PaymentType': ['CREDIT'],
        'PaymentBillingCode': ['Po0024697-20210127'],
        'PaymentDate': ['27/01/2021']
    })

    assert_frame_equal(expected_unique_orders, actual_unique_orders)


def test_get_orders():
    # Create a test dataframe with some data
    test_df = pd.DataFrame({
        'OrderNumber': ['Po0024697-1'],
        'ProductQuantity': [2],
        'UnitPrice': [2000.0],
        'PaymentType': ['CREDIT'],
        'PaymentBillingCode': ['Po0024697-20210127'],
        'PaymentDate': ['27/01/2021']
    })

    # Create an instance of MyClass using the test dataframe
    loader_df = OrdersLoader(test_df)

    # Call the get_customers method and check the results
    orders = loader_df.get_orders()
    assert isinstance(orders, list)
    assert len(orders) == 1
    for i, row in test_df.iterrows():
        assert orders[i].order_number == row['OrderNumber']
        assert orders[i].product_quantity == row['ProductQuantity']
        assert orders[i].unit_price == row['UnitPrice']
        assert orders[i].payment_type == row['PaymentType']
        assert orders[i].payment_billing_code == row['PaymentBillingCode']
        assert orders[i].payment_date == row['PaymentDate']
