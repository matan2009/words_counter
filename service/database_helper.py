import mysql.connector

from configurations.database_helper_configurations import DatabaseHelperConfigurations


def create_connection_to_db():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Nnn010203!",
        database="words_counter_schema"
    )
    cursor = conn.cursor()
    return conn, cursor


class DatabaseHelper(DatabaseHelperConfigurations):

    def __init__(self):
        super().__init__()
        self.conn, self.cursor = create_connection_to_db()

    def verify_db(self):
        # Check if the table exists
        query = "SHOW TABLES LIKE '{}'".format(self.config["table_name"])
        self.cursor.execute(query)
        if not self.cursor.fetchone():
            # create new table
            create_table_query = """CREATE TABLE words_counter (
            word VARCHAR(255) NOT NULL PRIMARY KEY,
            count INT NOT NULL)"""
            self.cursor.execute(create_table_query)
            self.conn.commit()

    def insert_to_db(self, row_info):
        keys = ', '.join(row_info.keys())
        values = ', '.join(['%s'] * len(row_info))
        query = f"INSERT INTO {self.config.table_name} ({keys}) VALUES ({values})"
        self.cursor.execute(query, tuple(row_info.values()))
        self.conn.commit()

    def update_database(self, words_counter_mapping: dict):
        try:
            update_query = "INSERT INTO words_counter (word, count) VALUES (%s, %s) ON DUPLICATE KEY UPDATE count = count + VALUES(count)"
            values = [(word, count) for word, count in words_counter_mapping.items()]
            self.cursor.executemany(update_query, values)
            self.conn.commit()
        except Exception as ex:
            print (ex)
