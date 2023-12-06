import unittest
import os
import pandas as pd
import sqlite3

class TestDataPipeline(unittest.TestCase):
    def test_data_pipeline(self):
        # Execute the data pipeline script
        os.system("python your_data_pipeline_script.py")

        # Validate the existence of the output SQLite files
        print("Checking existence of SQLite files...")
        self.assertTrue(os.path.exists("../data/global_temperature.sqlite"))
        self.assertTrue(os.path.exists("../data/crop_yield.sqlite"))

        # Check if tables exist in the databases
        print("Checking existence of tables in databases...")
        self.assertTrue(self.table_exists("../data/global_temperature.sqlite", "global_temperature"))
        self.assertTrue(self.table_exists("../data/crop_yield.sqlite", "crop_yield"))

        # Check if datasets are non-empty
        print("Checking if datasets are non-empty...")
        self.assertTrue(self.is_non_empty("../data/global_temperature.sqlite", "global_temperature"))
        self.assertTrue(self.is_non_empty("../data/crop_yield.sqlite", "crop_yield"))

        # Check if the global temperature dataset has the expected columns
        print("Checking columns in the global temperature dataset...")
        self.assertTrue(self.check_columns("../data/global_temperature.sqlite", "global_temperature", ["Year", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]))

        # Check if the crop yield dataset has the expected columns
        print("Checking columns in the crop yield dataset...")
        self.assertTrue(self.check_columns("../data/crop_yield.sqlite", "crop_yield", ["Area Code", "Area", "Item Code", "Item", "Year Code", "Year", "Value"]))

        # Check if the global temperature dataset has data for multiple years
        print("Checking if the global temperature dataset has data for multiple years...")
        self.assertTrue(self.check_multiple_years("../data/global_temperature.sqlite", "global_temperature"))

    def table_exists(self, database_path, table_name):
        # Check if a table exists in the specified SQLite database
        connection = sqlite3.connect(database_path)
        cursor = connection.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        result = cursor.fetchone()
        connection.close()
        return result is not None

    def is_non_empty(self, database_path, table_name):
        # Check if a table in the specified SQLite database is non-empty
        connection = sqlite3.connect(database_path)
        query = f"SELECT COUNT(*) FROM {table_name}"
        result = pd.read_sql_query(query, connection)
        connection.close()
        return result.iloc[0, 0] > 0

    def check_columns(self, database_path, table_name, expected_columns):
        # Check if the specified table has the expected columns
        connection = sqlite3.connect(database_path)
        cursor = connection.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [column[1] for column in cursor.fetchall()]
        connection.close()
        return set(expected_columns) == set(columns)

    def check_multiple_years(self, database_path, table_name):
        # Check if the global temperature dataset has data for multiple years
        connection = sqlite3.connect(database_path)
        query = f"SELECT COUNT(DISTINCT Year) FROM {table_name}"
        result = pd.read_sql_query(query, connection)
        connection.close()
        return result.iloc[0, 0] > 1

if _name_ == "_main_":
    unittest.main()
