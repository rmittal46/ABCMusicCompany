# ABCMusicCompany

This will be used to ingest the csv files in SQLite DB.

# Assumptions
Input csv's would be available under path **/resource/input_data**

# Configuration
**properties.yaml**
```
Where to locate above file : /resource/configs/properties.yaml

Contents of File :
file_path: resource/input_data/
file_name: orders.csv
```
# **Defination of propeties.yaml content**

file_path - provide the file path under resources folder
file_name - provide the file_name


**create_db.py**
```
Path to script : /resource/db_creation_script/create_db.py
This script is to create database for initial run.
This code have the capability to create database from the raw file.
```

**music_warehouse.db**
```
This file is available under resources directory. You can import db in SQLite using this file.
```

# How to Run the Project
To run the project, you need to have 
1. python installed in your machine.
2. Better to have Pycharm as IDE
3. SQLite DB

Once you have followed above steps , you can run the **main.py** either from IDE or from console.
Command to run application is : 

**python3 main.py -p properties.yaml**

# Future Scope
1. Add Unit tests
