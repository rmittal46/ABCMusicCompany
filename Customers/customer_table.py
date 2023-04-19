import pandas as pd
import sqlite3


class Customer:
    def __init__(self, first_name, last_name, email, phone):
        self.first_name = first_name
        self.last_name = last_name
        self.email = email
        self.phone = phone


class CustomersDB:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS customers (
                Customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
                First_name TEXT,
                Last_name Text,
                Email TEXT,
                Phone TEXT,
                IsActive BOOLEAN DEFAULT 1
            );
        ''')
        self.conn.commit()

    def insert_customer(self, customer_df):
        # define a SQL query to check for matching customer names
        query = "SELECT * FROM customers WHERE First_name = ? AND Last_name = ?"

        # loop over each row in the DataFrame and check for a matching customer in the database
        for index, row in customer_df.iterrows():
            params = (row['First_name'], row['Last_name'])
            existing_customer = pd.read_sql_query(query, self.conn, params=params)
            if not existing_customer.empty:
                print(f"Customer {params} already exists in database")
                pass
            else:
                # insert the new customer into the database
                row.to_sql('customers', self.conn, if_exists='append', index=False)

        self.conn.commit()


    def insert_new_customer(self):
        self.cursor.execute('''
            INSERT INTO customers
            (First_name, Last_name, IsActive)
            SELECT First_name, Last_name, 1
            FROM temp_customers
            WHERE NOT EXISTS (
                SELECT 1 FROM customers 
                WHERE customers.First_name = temp_customers.First_name and 
                      customers.Last_name = temp_customers.Last_name
            );
        ''')
        self.conn.commit()

    def close(self):
        self.conn.close()


class CustomerLoader:
    def __init__(self, file_path):
        self.customers_df = pd.read_csv(file_path)

    def split_customer_name(self, dataframe):
        dataframe[['First_name', 'Last_name']] = self.customers_df['ClientName'].str.split(n=1, expand=True)
        return dataframe.loc[:, ['First_name', 'Last_name']]

    def get_unique_customers(self):
        self.customers_df['ClientName'] = self.customers_df['ClientName'].str.replace('[^a-zA-Z0-9 ]+', '', regex=True)
        return self.customers_df.drop_duplicates(subset=['ClientName']).loc[:, ['ClientName']]

    def get_customers(self):
        customers = []
        for _, row in self.customers_df.iterrows():
            customer = Customer(row['First_name'], row['Last_name'], row['Email'], row['Phone'],
                                row['IsActive'])
            customers.append(customer)
        return customers
