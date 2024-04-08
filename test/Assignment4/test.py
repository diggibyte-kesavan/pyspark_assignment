import unittest
from pyspark.sql import SparkSession
from src.Assignment4.util  import read_json, flatten_df, to_snake_case, add_load_date_with_current_date, add_year_month_day


class TestJsonProcessing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("assignment4") .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_read_json(self):
        json_path = r'C:\Users\kesav\Documents\DE_Intern_Diggibyte\dataset\nested_json_file.json'
        df = read_json(self.spark, json_path)
        self.assertIsNotNone(df)
        # Add more assertions based on the structure of your JSON data

    def test_flatten_df(self):
        json_path = r'C:\Users\kesav\Documents\DE_Intern_Diggibyte\dataset\nested_json_file.json'
        df = read_json(self.spark, json_path)
        flattened_df = flatten_df(df)
        self.assertIsNotNone(flattened_df)
        # Add more assertions based on the expected flattened DataFrame structure

    def test_to_snake_case(self):
        json_path = r'C:\Users\kesav\Documents\DE_Intern_Diggibyte\dataset\nested_json_file.json.'
        df = read_json(self.spark, json_path)
        snake_case_df = to_snake_case(df)
        self.assertIsNotNone(snake_case_df)
        # Add more assertions based on the expected column names

    def test_add_load_date_with_current_date(self):
        json_path = r'C:\Users\kesav\Documents\DE_Intern_Diggibyte\dataset\nested_json_file.json'
        df = read_json(self.spark, json_path)
        load_date_df = add_load_date_with_current_date(df)
        self.assertIsNotNone(load_date_df)
        # Add more assertions based on the presence of the load_date column

    # def test_add_year_month_day(self):
    #     json_path = r'C:\Users\kesav\Documents\DE_Intern_Diggibyte\dataset\nested_json_file.json'
    #     df = read_json(self.spark, json_path)
    #     year_month_day_df = add_year_month_day(df)
    #     self.assertIsNotNone(year_month_day_df)
    #     # Add more assertions based on the presence  the year, month, and day columns


if __name__ == "__main__":
    unittest.main()