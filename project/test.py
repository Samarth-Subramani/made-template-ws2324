import unittest
import pandas as pd
import sqlite3
from zipfile import ZipFile
import requests
from io import BytesIO

class TestSetupSQLiteDatabases(unittest.TestCase):

   def setUp(self):
    try:
        # Set up SQLite databases for NASA dataset
        self.db_path1 = 'global_temperature.sqlite'
        self.conn1 = sqlite3.connect(self.db_path1)
        self.table1 = 'global_temperature'
        self.columns1 = self.get_table_columns(self.conn1, self.table1)
        print(f"Columns in {self.table1}: {self.columns1}")

        # Set up SQLite databases for FAO dataset
        self.db_path2 = 'crop_yield.sqlite'
        self.conn2 = sqlite3.connect(self.db_path2)
        self.table2 = 'crop_yield'
        self.columns2 = self.get_table_columns(self.conn2, self.table2)
        print(f"Columns in {self.table2}: {self.columns2}")

        self.fao_data_df = pd.read_sql_query(f"SELECT * FROM {self.table2};", self.conn2)
    except Exception as e:
        self.fail(f"Failed to set up test environment: {e}")

    def get_table_columns(self, connection, table_name):
        cursor = connection.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [column[1] for column in cursor.fetchall()]
        return columns

    def test_fao_dataset_exists(self):
        try:
            # Test if the crop_yield table exists in the database
            cursor = self.conn2.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.table2}';")
            tables = cursor.fetchall()
            table_names = [table[0] for table in tables]
            self.assertIn(self.table2, table_names, f"Test failed: {self.table2} table does not exist in the database.")
            print(f"Test passed: {self.table2} table exists in the database.")
        except Exception as e:
            self.fail(f"Test failed: {e}")

    def test_nasa_dataset_exists(self):
        try:
            # Test if the global_temperature table exists in the database
            cursor = self.conn1.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.table1}';")
            tables = cursor.fetchall()
            table_names = [table[0] for table in tables]
            self.assertIn(self.table1, table_names, f"Test failed: {self.table1} table does not exist in the database.")
            print(f"Test passed: {self.table1} table exists in the database.")
        except Exception as e:
            self.fail(f"Test failed: {e}")
    
    def test_fao_data_rows(self):
        try:
            # Test if the number of rows in the FAO dataset is greater than zero
            self.assertGreater(len(self.fao_data_df), 0, "Number of rows in FAO dataset is not greater than zero.")
            print("Test passed: Number of rows in FAO dataset is greater than zero.")
        except Exception as e:
            self.fail(f"Test failed: {e}")

    def test_nasa_data_rows(self):
        try:
            # Test if the number of rows in the NASA dataset is greater than zero
            cursor = self.conn1.cursor()
            query = f"SELECT COUNT(*) FROM {self.table1};"
            cursor.execute(query)
            row_count = cursor.fetchone()[0]
            self.assertGreater(row_count, 0, "Number of rows in NASA dataset is not greater than zero.")
            print("Test passed: Number of rows in NASA dataset is greater than zero.")
        except Exception as e:
            self.fail(f"Test failed: {e}")

if __name__ == '__main__':
    unittest.main()
