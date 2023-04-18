# Functions that are going to help ingestion, transformation & cleanup of file
import sqlite3

import os
import pandas as pd
import yaml
from Utils.logger import getlogger

logger = getlogger(__name__)

# load properties file
def load_yaml(args):
    with open(os.path.join(getPath(), 'resource/configs/',args.properties), "r") as f:
        config = yaml.safe_load(f)
    logger.info("Loading properties file : %s", args.properties)
    return config


def load_file(filepath):
    # Read the CSV file into a DataFrame
    logger.info("Reading the csv file started")
    orders = pd.read_csv(os.path.join(getPath(), filepath))
    logger.info("Reading the csv file completed")
    return orders


def getPath():
    path = os.getcwd()
    return path


def ingestToDb(datamodel, tablename, primaryKey):
    # create a new SQLite database
    logger.info('creating db connection')
    conn = sqlite3.connect(os.path.join(getPath(), 'resource/music_warehouse.db'))

    temp_table = tablename + '_temp'

    logger.info("creating temp_table %s in db ", temp_table)
    datamodel.to_sql(temp_table, conn, if_exists='replace', index=False)
    logger.info("temp_table %s creation completed ", temp_table)

    cur = conn.cursor()
    # df = pd.read_sql('select * from orders_temp', conn)

    sql_statement = '''Insert into ''' + tablename + ''' Select * from ''' + temp_table + ''' as O where O.''' + primaryKey + ''' Not In (select  ''' + primaryKey + ''' from ''' + tablename + ''')'''
    logger.info("sql to ingest data in table %s is : %s",tablename, sql_statement)

    cur.execute(sql_statement)
    logger.info("Data Ingested in table : %s ", tablename)

    logger.info("dropping temp table starts")
    cur.execute(dropTable(tablename))
    logger.info("Temp table %s dropped", temp_table)

    # Closes the connection
    conn.close()


def dropTable(tableName):
    drop_table_sql = 'drop table ' + tableName
    return drop_table_sql