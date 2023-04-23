import pandas as pd
import pytest

from DataModels.Database_details import Database
from DataModels.customer_table import CustomersDB, CustomerLoader, Customer
from utils.logger import getlogger
from pandas.testing import assert_frame_equal

logger = getlogger(__name__)


def setup_env():
    # prepare a test database with some initial data
    db_name = ':memory:'

    # create a Database instance to initiate db connection and cursor
    db = Database(db_name)
    return db


def test_insert_customer():
    db = setup_env()
    # create a CustomersDB object and call the insert_customer method with some new customers
    customer = CustomersDB(db)

    customer.create_table()

    db.cursor.execute("INSERT INTO customers (First_name, Last_name, IsActive) VALUES ('Alice', 'Adams', 1)")

    df = pd.DataFrame({'First_name': ['Bob', 'Charlie'], 'Last_name': ['Brown', 'Chaplin']})
    customer.insert_customer(df)

    # check that the new customers were inserted correctly into the database
    db.cursor.execute('SELECT * FROM customers')

    result = db.cursor.fetchall()

    expected = [(1, 'Alice', 'Adams', None, None, 1), (2, 'Bob', 'Brown', None, None, 1),
                (3, 'Charlie', 'Chaplin', None, None, 1)]
    print("result ", result)
    print("expected ", expected)

    customer.close()

    assert result == expected


def test_unique_customers():
    df = pd.DataFrame({'ClientName': ['Bob Brown', 'Bob Brown', 'Bobby Dave'],
                       'First_name': ['Bob', 'Bob', 'Bobby'],
                       'Last_name': ['Brown', 'Brown', 'Dave']
                       })

    loader_df = CustomerLoader(df)

    actual_unique_customers = loader_df.get_unique_customers()

    expected_unique_customers = pd.DataFrame({'First_name': ['Bob', 'Bobby'],
                                              'Last_name': ['Brown', 'Dave']
                                              })

    assert_frame_equal(expected_unique_customers, actual_unique_customers)


def test_get_customers():
    # Create a test dataframe with some data
    test_data = {
        'First_name': ['John', 'Jane'],
        'Last_name': ['Doe', 'Smith'],
        'Email': ['johndoe@example.com', 'janesmith@example.com'],
        'Phone': ['1234567890', '0987654321'],
        'IsActive': [True, False]
    }
    test_df = pd.DataFrame(test_data)

    # Create an instance of MyClass using the test dataframe
    loader_df = CustomerLoader(test_df)

    # Call the get_customers method and check the results
    customers = loader_df.get_customers()
    assert isinstance(customers, list)
    assert len(customers) == 2

    # Check the first customer object
    assert isinstance(customers[0], Customer)
    assert customers[0].first_name == 'John'
    assert customers[0].last_name == 'Doe'
    assert customers[0].email == 'johndoe@example.com'
    assert customers[0].phone == '1234567890'
    assert customers[0].is_active == True

    # Check the second customer object
    assert isinstance(customers[1], Customer)
    assert customers[1].first_name == 'Jane'
    assert customers[1].last_name == 'Smith'
    assert customers[1].email == 'janesmith@example.com'
    assert customers[1].phone == '0987654321'
    assert customers[1].is_active == False
