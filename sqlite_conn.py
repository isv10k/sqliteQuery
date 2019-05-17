import sqlite3 as lite


class SqliteConn:

    def __init__(self, db_name):

        self.db_name = db_name

    def __enter__(self):

        self.conn = lite.connect(self.db_name)
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):

        self.conn.close()
        if exc_val:
            raise
