# ABCMusicCompany

This will be used to ingest the csv files in SQLite DB.

# Assumptions
Input csv's would be available under path **/resource/input_data**

# Configuration
**properties.yaml**
```
table_name: orders
file_path: resource/input_data/
file_name: orders_1.csv
primary_key: OrderNumber
```
table_name - provide the table name without quotes.
file_path - provide the file path under resources folder
file_name - provide the file_name
primary_key - need to define the primary key here

So basically for every csv file_load of different schema, we need to maintain multiple properties file.

**create_db.py**
```
This script is to create database for initial run.
This code have the capability to create database from the raw file.
```

**music_warehouse.db**
```
This file is available under Datamodels directory. You can load import db in SQLite using this file.
```

# How to Run the Project
To run the project, you need to have 
1. python installed in your machine.
2. Better to have Pycharm as IDE
3. SQLite DB

Once you have followed above steps , you can run the **main.py** either from IDE or from console.
Currently, I have properties.yaml updated to load orders data in db.

# Future Scope
1. Add Unit tests
