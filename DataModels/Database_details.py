import sqlite3

from utils.logger import getlogger

logger = getlogger(__name__)


class Database:

    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

    def close(self):
        self.conn.close()