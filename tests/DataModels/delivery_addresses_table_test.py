import pandas as pd
from pandas._testing import assert_frame_equal

from DataModels.customer_table import CustomersDB
from DataModels.delivery_addresses_table import DeliveryAddressDB, AddressLoader


def test_insert_address():
    # prepare a test database with some initial data
    db_name = ':memory:'

    # create a CustomersDB object and call the insert_customer method with some new Addresses
    db = DeliveryAddressDB(db_name)
    db.cursor.execute('''INSERT INTO delivery_addresses (Customer_id, Address_line, DeliveryCity, 
                    DeliveryPostcode, DeliveryCountry, DeliveryContactNumber, CurrentFlag, EffectiveFrom, EffectiveTo) 
                    VALUES (1, '72 Academy Street', 'Swindon', 'SN4 9QP', 'United Kingdom', '+44 7911 843910', 1, 
                            '2023-04-20', '9999-12-31')''')

    test_df = pd.DataFrame({
        'Customer_id': [1, 1, 2],
        'DeliveryAddress': ['123 Main St', '456 Oak Ave', '789 Maple Dr'],
        'DeliveryCity': ['Anytown', 'Anycity', 'Anystate'],
        'DeliveryPostcode': ['12345', '67890', '45678'],
        'DeliveryCountry': ['USA', 'USA', 'Canada'],
        'DeliveryContactNumber': ['555-1234', '555-5678', '555-9012']
    })

    db.insert_address(test_df)

    # retrieve the inserted data from the database
    query = "SELECT * FROM delivery_addresses WHERE Customer_id = ? and DeliveryPostcode = ?"
    params = (1, '12345')
    result = pd.read_sql_query(query, db.conn, params=params)

    # assert that the data was inserted correctly
    assert result.shape[0] == 1
    assert result['Address_line'].values[0] == '123 Main St'
    assert result['DeliveryCity'].values[0] == 'Anytown'
    assert result['DeliveryCountry'].values[0] == 'USA'
    assert result['DeliveryContactNumber'].values[0] == '555-1234'

    # clean up by deleting the test database
    db.close()


def test_get_address():
    # Create a mock DataFrame with some test data
    test_df = pd.DataFrame({
        'DeliveryAddress': ['123 Main St', '456 Oak Ave', '789 Maple Dr'],
        'DeliveryCity': ['Anytown', 'Anycity', 'Anystate'],
        'DeliveryPostcode': ['12345', '67890', '45678'],
        'DeliveryCountry': ['USA', 'USA', 'Canada'],
        'DeliveryContactNumber': ['555-1234', '555-5678', '555-9012']
    })

    # Create an instance of MyClass using the test dataframe
    loader_df = AddressLoader(test_df)

    # Call the get_address method and assert that it returns a list of DeliveryAddress objects
    addresses = loader_df.get_address()
    assert isinstance(addresses, list)
    assert len(addresses) == test_df.shape[0]
    for i, row in test_df.iterrows():
        assert addresses[i].address_line == row['DeliveryAddress']
        assert addresses[i].delivery_city == row['DeliveryCity']
        assert addresses[i].delivery_postcode == row['DeliveryPostcode']
        assert addresses[i].delivery_country == row['DeliveryCountry']
        assert addresses[i].delivery_contact_number == row['DeliveryContactNumber']


def test_unique_address():
    df = pd.DataFrame({'DeliveryAddress': ['72 Academy Street', '91 Buckingham Rd', '72 Academy Street'],
                       'DeliveryPostcode': ['SN4 9QP', 'DN37 9TS', 'SN4 9QP'],
                       'DeliveryCity': ['Swindon', 'Grimsby', 'Swindon'],
                       'DeliveryCountry': ['United Kingdom', 'United Kingdom', 'United Kingdom'],
                       'First_name': ['Bob', 'Bobby', 'Bob'],
                       'Last_name': ['Brown', 'Dave', 'Brown'],
                       'ClientName': ['Bob Brown', 'Bobby Dave', 'Bob Brown'],
                       'DeliveryContactNumber': ['+44 7911 843910', '+44 7911 843919', '+44 7911 843910']
                       })

    loader_df = AddressLoader(df)

    actual_unique_address = loader_df.get_unique_address()

    expected_unique_address = pd.DataFrame(
        {'ClientName': ['Bob Brown', 'Bobby Dave'],
         'DeliveryAddress': ['72 Academy Street', '91 Buckingham Rd'],
         'DeliveryCity': ['Swindon', 'Grimsby'],
         'DeliveryPostcode': ['SN4 9QP', 'DN37 9TS'],
         'DeliveryCountry': ['United Kingdom', 'United Kingdom'],
         'DeliveryContactNumber': ['+44 7911 843910', '+44 7911 843919'],
         'First_name': ['Bob', 'Bobby'],
         'Last_name': ['Brown', 'Dave']
         })

    assert_frame_equal(expected_unique_address, actual_unique_address)


def test_get_customers():
    # prepare a test database with some initial data
    db_name = ':memory:'

    db = CustomersDB(db_name)
    db.cursor.execute("INSERT INTO customers (First_name, Last_name, IsActive) VALUES ('Bob', 'Brown', 1)")

    # Create a test dataframe with some data
    test_data = pd.DataFrame({'DeliveryAddress': ['72 Academy Street', '91 Buckingham Rd', '72 Academy Street'],
                              'DeliveryPostcode': ['SN4 9QP', 'DN37 9TS', 'SN4 9QP'],
                              'DeliveryCity': ['Swindon', 'Grimsby', 'Swindon'],
                              'DeliveryCountry': ['United Kingdom', 'United Kingdom', 'United Kingdom'],
                              'First_name': ['Bob', 'Bobby', 'Bob'],
                              'Last_name': ['Brown', 'Dave', 'Brown'],
                              'ClientName': ['Bob Brown', 'Bobby Dave', 'Bob Brown'],
                              'DeliveryContactNumber': ['+44 7911 843910', '+44 7911 843919', '+44 7911 843910']
                              })

    loader_df = AddressLoader(test_data)
    unique_address = loader_df.get_unique_address()

    address_df = loader_df.getCustomer_id(unique_address, db)

    assert address_df['Customer_id'].values[0] == [1]
