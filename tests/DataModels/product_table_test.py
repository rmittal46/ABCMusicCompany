import pandas as pd
import pytest

from DataModels.Database_details import Database
from DataModels.product_table import ProductsDB, ProductLoader
from utils.logger import getlogger
from pandas.testing import assert_frame_equal

logger = getlogger(__name__)


def setup_env():
    # prepare a test database with some initial data
    db_name = ':memory:'

    # create a Database instance to initiate db connection and cursor
    db = Database(db_name)
    return db


def test_insert_products():
    db = setup_env()
    # create a ProductsDB object and call the insert_product method with some new products
    product = ProductsDB(db)

    product.create_table()

    db.cursor.execute('''INSERT INTO products (ProductName, ProductType, UnitPrice, 
                    ProductQuantity, Currency, CurrentFlag, EffectiveFrom, EffectiveTo) 
                    VALUES ('Piano','Keyboard',4700,10,'GBP',1,'2023-04-20', '9999-12-31')
                    ''')

    df = pd.DataFrame({
        'ProductName': ['Harp', 'Zither'],
        'ProductType': ['String', 'String'],
        'UnitPrice': [940.0, 2400.0],
        'ProductQuantity': ['2', '1'],
        'Currency': ['GBP', 'GBP']
    })
    product.insert_product(df)

    # check that the new customers were inserted correctly into the database
    result = pd.read_sql_query('SELECT * FROM products', db.conn)

    # assert that the data was inserted correctly
    assert result.shape[0] == 3
    assert result['ProductName'].values[0] == 'Piano'
    assert result['ProductType'].values[0] == 'Keyboard'
    assert result['UnitPrice'].values[0] == 4700.0
    assert result['ProductQuantity'].values[0] == 10
    assert result['Currency'].values[0] == 'GBP'

    db.close()


def test_unique_products():
    df = pd.DataFrame({'ProductName': ['Piano', 'Bamboo Flute', 'Piano'],
                       'ProductType': ['Keyboard', 'Woodwind', 'Keyboard'],
                       'UnitPrice': [4700.0, 60.0, 4700.0],
                       'Currency': ['GBP', 'GBP', 'GBP'],
                       'ClientName': ['Macgyver inc', 'Howell llc', 'Macgyver inc']
                       })

    loader_df = ProductLoader(df)

    actual_unique_customers = loader_df.get_unique_products()

    expected_unique_customers = pd.DataFrame({'ProductName': ['Piano', 'Bamboo Flute'],
                                              'ProductType': ['Keyboard', 'Woodwind'],
                                              'UnitPrice': [4700.0, 60.0],
                                              'Currency': ['GBP', 'GBP']
                                              })

    assert_frame_equal(expected_unique_customers, actual_unique_customers)


def test_get_products():
    # Create a test dataframe with some data
    test_df = pd.DataFrame({'ProductName': ['Piano', 'Bamboo Flute'],
                            'ProductType': ['Keyboard', 'Woodwind'],
                            'UnitPrice': [4700.0, 60.0],
                            'ProductQuantity': [10, 22],
                            'Currency': ['GBP', 'GBP']
                            })

    # Create an instance of MyClass using the test dataframe
    loader_df = ProductLoader(test_df)

    # Call the get_customers method and check the results
    products = loader_df.get_products()
    assert isinstance(products, list)
    assert len(products) == 2
    for i, row in test_df.iterrows():
        assert products[i].product_name == row['ProductName']
        assert products[i].product_type == row['ProductType']
        assert products[i].unit_price == row['UnitPrice']
        assert products[i].product_quantity == row['ProductQuantity']
        assert products[i].currency == row['Currency']


def test_get_product_quantity_by_product():
    # Create a test dataframe with some data
    test_data = pd.DataFrame({'ProductName': ['Piano', 'Bamboo Flute', 'Piano'],
                              'ProductType': ['Keyboard', 'Woodwind', 'Keyboard'],
                              'UnitPrice': [4700.0, 60.0, 4700.0],
                              'Currency': ['GBP', 'GBP', 'GBP'],
                              'ClientName': ['Macgyver inc', 'Howell llc', 'Macgyver inc'],
                              'ProductQuantity': [2, 4, 5]
                              })

    loader_df = ProductLoader(test_data)

    Product_quantity = loader_df.get_product_quantity_by_product()

    assert Product_quantity['ProductQuantity'].values[1] == [7]


def test_update_products():
    db = setup_env()

    # create a ProductsDB object and call the insert_product method with some new products
    products = ProductsDB(db)

    products.create_table()

    df = pd.DataFrame({
        'ProductName': ['Harp'],
        'ProductType': ['String'],
        'UnitPrice': [1000.0],
        'ProductQuantity': ['4'],
        'Currency': ['GBP']
    })
    products.insert_product(df)

    # check that the new products were inserted correctly into the database
    result = pd.read_sql_query('''SELECT * FROM products WHERE ProductName = "Harp" and CurrentFlag = 1 ''', db.conn)

    # assert that the data was inserted correctly
    assert result.shape[0] == 1
    assert result['ProductName'].values[0] == 'Harp'
    assert result['ProductType'].values[0] == 'String'
    assert result['UnitPrice'].values[0] == 1000.0
    assert result['ProductQuantity'].values[0] == 4
    assert result['Currency'].values[0] == 'GBP'
    assert result['CurrentFlag'].values[0] == 1

    db.close()
