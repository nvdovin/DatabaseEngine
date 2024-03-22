import pandas as pd
from postgres_db import PostgresDB
from postgres_db import Type
import secret_words as s


class ExcelWorker:
    def __init__(self, path_to_file):
        self.path_to_file = path_to_file
        self.excel_file = pd.ExcelFile(self.path_to_file)
        self.sheets_list = self.excel_file.sheet_names
        self.df = pd.DataFrame()
    
    def find_table_from_sheet(self, sheet_name: str):
        # sourcery skip: sum-comprehension
        """
        Finds and returns a table from a specified sheet in the Excel file.

        :param sheet_name: The name of the sheet to search for.
        :type sheet_name: str
        :return: A pandas DataFrame representing the table found in the sheet.
        :rtype: pandas.DataFrame
        """

        self.df = pd.read_excel(self.path_to_file, sheet_name=sheet_name, header=1)
        for column in self.df.columns: # Remove empty columns from self.df.columns:
            empty_rows = 0
            for value in self.df[column]:
                if pd.isna(value):
                    empty_rows += 1
            if empty_rows == len(self.df[column]):
                self.df = self.df.drop(column, axis=1)
        self.guess_types_of_columns()
    
    def guess_types_of_columns(self):
        column_types = {}

        for column in self.df.columns:
            current_column_types = [type(value) for value in self.df[column]]
            if str in current_column_types:
                column_types[f"{column}"] = Type.CHAR
            elif float in current_column_types:
                column_types[f"{column}"] = Type.FLOAT
            elif int in current_column_types:
                column_types[f"{column}"] = Type.INT
            elif bool in current_column_types:
                column_types[f"{column}"] = Type.BOOL
        print(column_types)

    def write_to_database(self, table_name: str, columns: list, values: list):
        columns_dict = {}
        """ Вернуться и доделать! """
        psql = PostgresDB(dbname="test_database", user=s.POSTGRES_USER, password=s.POSTGRES_PASSWORD)
        psql.create_table(title=table_name, columns=columns)


xls = ExcelWorker(path_to_file="test.xlsx")
xls.find_table_from_sheet("2024")

