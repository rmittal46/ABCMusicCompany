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
cur.execute('''CREATE TABLE IF NOT EXISTS customers (
                Customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
                First_name TEXT,
                Last_name Text,
                Email TEXT,
                Phone TEXT,
                IsActive INTEGER DEFAULT 1
            )''')

# Create a table for products
cur.execute('''CREATE TABLE IF NOT EXISTS products (
                Product_id INTEGER PRIMARY KEY AUTOINCREMENT,
                ProductName TEXT,
                ProductType TEXT,
                ProductQuantity INTEGER,
                UnitPrice REAL,
                Currency TEXT,
                EffectiveFrom DATETIME DEFAULT CURRENT_TIMESTAMP,
                EffectiveTo DATETIME DEFAULT '9999-12-31 23:59:59.999',
                CurrentFlag INTEGER DEFAULT 1
            )''')

# Create a table for orders
cur.execute('''CREATE TABLE IF NOT EXISTS orders (
                OrderNumber TEXT PRIMARY KEY,
                ProductQuantity INTEGER,
                UnitPrice NUMERIC,
                PaymentType TEXT,
                PaymentBillingCode TEXT,
                PaymentDate DATE
)''')


# Create a table for deliveryAddress
cur.execute('''CREATE TABLE IF NOT EXISTS delivery_addresses (
                Address_id INTEGER PRIMARY KEY AUTOINCREMENT,
                Customer_id INTEGER,
                Address_line TEXT,
                DeliveryCity TEXT,
                DeliveryPostcode TEXT,
                DeliveryCountry TEXT,
                DeliveryContactNumber TEXT,
                EffectiveFrom DATETIME DEFAULT CURRENT_TIMESTAMP,
                EffectiveTo DATETIME DEFAULT '9999-12-31 23:59:59.999',
                CurrentFlag INTEGER DEFAULT 1,
                FOREIGN KEY (Customer_id) REFERENCES customers (Customer_id)
)''')

# Create a table for order items
cur.execute('''CREATE TABLE IF NOT EXISTS order_details (
                OrderNumber TEXT,
                Customer_id INTEGER,
                Product_id INTEGER,
                Address_id TEXT,
                PRIMARY KEY (OrderNumber, Customer_id, Product_id, Address_id),
                FOREIGN KEY (OrderNumber) REFERENCES orders (OrderNumber),
                FOREIGN KEY (Product_id) REFERENCES products (Product_id),
                FOREIGN KEY (Customer_id) REFERENCES customers (Customer_id),
                FOREIGN KEY (Address_id) REFERENCES delivery_address (Address_id)
            )''')

# Commit the changes to the database and close the connection
conn.commit()
conn.close()