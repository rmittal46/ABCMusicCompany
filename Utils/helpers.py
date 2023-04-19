# Functions that are going to help ingestion, transformation & cleanup of file
import sqlite3

import os
import pandas as pd
import yaml
from Utils.logger import getlogger

logger = getlogger(__name__)


# load properties file
def load_yaml(args):
    with open(os.path.join(getPath(), 'resource/configs/', args.properties), "r") as f:
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


def dropTable(tablename):
    drop_table_sql = 'drop table ' + tablename
    logger.info("%s ", drop_table_sql)
    return drop_table_sql
