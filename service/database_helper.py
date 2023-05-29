import mysql.connector
from mysql.connector import CMySQLConnection
import configparser

from configurations.words_counter_configurations import WordsCounterConfigurations


class DatabaseHelper(WordsCounterConfigurations):

    def __init__(self):
        super().__init__()
        self.conn = None
        self.cursor = None

    def create_connection_to_mysql_server(self) -> (CMySQLConnection, str, str, str):
        db_config = configparser.ConfigParser()
        db_config.read(self.config["database_helper"]["db_config_filename"])
        host = self.config["database_helper"]["host"]
        user_name = db_config['Credentials']['DB_USERNAME']
        password = db_config['Credentials']['DB_PASSWORD']
        # database = self.config["database_helper"]["database_name"]
        conn = mysql.connector.connect(
            host=host,
            user=user_name,
            password=password,
        )
        return conn, host, user_name, password

    def verify_database(self, server_conn: CMySQLConnection):
        # Check if the database exists
        cursor = server_conn.cursor()
        database_name = self.config['database_helper']['database_name']
        query = "SHOW DATABASES"
        cursor.execute(query)
        databases = cursor.fetchall()
        for database in databases:
            if database[0] == database_name:
                # database already exists
                return
        # create new database
        create_schema_query = f"CREATE DATABASE {database_name}"
        cursor.execute(create_schema_query)

    def create_connection_to_database(self, host: str, user_name: str, password: str) -> CMySQLConnection:
        database_name = self.config["database_helper"]["database_name"]
        conn = mysql.connector.connect(
            host=host,
            user=user_name,
            password=password,
            database=database_name
        )
        return conn

    def verify_table(self, db_conn: CMySQLConnection):
        # Check if the table exists
        cursor = db_conn.cursor()
        table_name = self.config['database_helper']['table_name']
        query = f"SHOW TABLES LIKE '{table_name}'"
        cursor.execute(query)
        if not cursor.fetchone():
            # create new table
            create_table_query = f"""CREATE TABLE {table_name} (
            word VARCHAR(255) NOT NULL PRIMARY KEY,
            count INT NOT NULL)"""
            cursor.execute(create_table_query)
            db_conn.commit()
        self.conn = db_conn
        self.cursor = cursor

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
