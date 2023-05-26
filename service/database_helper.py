import mysql.connector
import configparser

from configurations.words_counter_configurations import WordsCounterConfigurations


def create_connection_to_db(configurations):
    db_config = configparser.ConfigParser()
    db_config.read(configurations["database_helper"]["db_config_filename"])
    host = configurations["database_helper"]["host"]
    database = configurations["database_helper"]["database"]
    conn = mysql.connector.connect(
        host=host,
        user=db_config['Credentials']['DB_USERNAME'],
        password=db_config['Credentials']['DB_PASSWORD'],
        database=database
    )
    cursor = conn.cursor()
    return conn, cursor


class DatabaseHelper(WordsCounterConfigurations):

    def __init__(self):
        super().__init__()
        self.conn, self.cursor = create_connection_to_db(self.config)

    def verify_db(self):
        # Check if the table exists
        query = "SHOW TABLES LIKE '{}'".format(self.config["database_helper"]["table_name"])
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
        update_query = "INSERT INTO words_counter (word, count) VALUES (%s, %s) ON DUPLICATE KEY UPDATE count = count + VALUES(count)"
        values = [(word, count) for word, count in words_counter_mapping.items()]
        self.cursor.executemany(update_query, values)
        self.conn.commit()

    def get_count_from_db(self, word: str) -> int:
        find_query = "SELECT * FROM words_counter WHERE word = %s"
        self.cursor.execute(find_query, (word, ))
        row = self.cursor.fetchone()
        if row:
            return row[1]
        return 0

