import sqlite3
import os
from Utils.constants import database_name

path = os.getcwd()

# Connect to the database (creates a new file if it doesn't exist)
db_path = '../resource/' + database_name
conn = sqlite3.connect(os.path.join(path, db_path))

# Create a cursor object to execute SQL commands
cur = conn.cursor()

# Create a table for customers
cur.execute('''CREATE TABLE customers (
                Customer_id INTEGER PRIMARY KEY,
                First_name TEXT,
                Last_name Text,
                Email TEXT,
                Phone TEXT,
                Address TEXT,
                EffectiveFrom DATETIME DEFAULT CURRENT_TIMESTAMP,
                EffectiveTo DATETIME DEFAULT '9999-12-31 23:59:59.999',
                CurrentFlag BOOLEAN DEFAULT 1
            )''')

# Create a table for products
cur.execute('''CREATE TABLE products (
                Product_id INTEGER PRIMARY KEY,
                ProductName TEXT,
                ProductType TEXT,
                ProductQuantity INTEGER,
                UnitPrice REAL,
                Currency TEXT,
                EffectiveFrom DATETIME DEFAULT CURRENT_TIMESTAMP,
                EffectiveTo DATETIME DEFAULT '9999-12-31 23:59:59.999',
                CurrentFlag BOOLEAN DEFAULT 1
            )''')

# Create a table for orders
cur.execute('''CREATE TABLE orders (
    OrderNumber TEXT,
    ClientName TEXT,
    ProductName TEXT,
    ProductType TEXT,
    UnitPrice NUMERIC,
    ProductQuantity INTEGER,
    TotalPrice NUMERIC,
    Currency TEXT,
    DeliveryAddress TEXT,
    DeliveryCity TEXT,
    DeliveryPostcode TEXT,
    DeliveryCountry TEXT,
    DeliveryContactNumber TEXT,
    PaymentType TEXT,
    PaymentBillingCode TEXT,
    PaymentDate DATE,
    PRIMARY KEY (OrderNumber, Customer_id),
    FOREIGN KEY (Customer_id) REFERENCES customers (Customer_id)
)''')

# Create a table for order items
cur.execute('''CREATE TABLE order_items (
                OrderNumber INTEGER,
                ProductName INTEGER,
                quantity INTEGER,
                PRIMARY KEY (OrderNumber, ProductName),
                FOREIGN KEY (OrderNumber) REFERENCES orders (OrderNumber),
                FOREIGN KEY (ProductName) REFERENCES products (Product_id)
            )''')

# Commit the changes to the database and close the connection
conn.commit()
conn.close()