import pandas as pd
from postgres_db import PostgresDB
from postgres_db import Type
import secret_words as s

from datetime import datetime


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
    
    def guess_types_of_columns(self):  # sourcery skip: merge-else-if-into-elif
        column_types = {}

        for column in self.df.columns:
            current_column_types = []
            for value in self.df[column]:
                print(value, type(value))
                if type(str(value).isdigit()) == int:  # noqa: E721
                    current_column_types.append("int")
                else:
                    if self.is_decimal(str(value)):  # noqa: E721
                        current_column_types.append("float")
                    elif self.is_date(str(value)):
                        current_column_types.append("date")
                    elif str(value) == "nan":
                        current_column_types.append(None)
                    else:
                        if len(str(value)) < 200:
                            current_column_types.append("char")
                        else:
                            current_column_types.append("text")
                        

            if "float" in current_column_types:
                column_types[f"{column}"] = Type.FLOAT
            elif "int" in current_column_types:
                column_types[f"{column}"] = Type.INT
            elif "text" in current_column_types:
                column_types[f"{column}"] = Type.TEXT
            elif "char" in current_column_types:
                column_types[f"{column}"] = Type.CHAR
            elif "bool" in current_column_types:
                column_types[f"{column}"] = Type.BOOL
            elif "date" in current_column_types:
                column_types[f"{column}"] = Type.DATE
            else:
                column_types[f"{column}"] = Type.TEXT
        
        return ', '.join([f'"{key}" {value}' for key, value in column_types.items()])


    def write_to_database(self, table_name: str):
        column_types = self.guess_types_of_columns()
        print(f"Колонны: {column_types}")
        """ Вернуться и доделать! """
        psql = PostgresDB(dbname="test_database", user=s.POSTGRES_USER, password=s.POSTGRES_PASSWORD)
        # psql.create_table(title=table_name, columns=column_types)

    @staticmethod
    def is_decimal(value):
        """
        Check if the given value is a decimal number.

        Parameters:
            value (str): The value to be checked.

        Returns:
            bool: True if the value is a decimal number, False otherwise.
        """
        dots_counter = 0
        numbers_counter = 0
        total_len = len(value)
        total_number = ""

        for char in value:
            if char in [".", ","]:
                dots_counter += 1
                total_number += "."
            elif char.isdigit():
                numbers_counter += 1
                total_number += char
            else:
                return False
        return dots_counter == 1 and numbers_counter == total_len - 1

    @staticmethod
    def is_date(value):
        """
        Check if the input value is a valid date in the format '%Y-%m-%d' or '%d.%m.%Y', and return True if it is, otherwise return False.
        """
        try:
            try:
                datetime.strptime(value, '%Y-%m-%d')
                return True
            except ValueError:
                datetime.strptime(value, '%d.%m.%Y')
                return True
        except ValueError:
            return False


xls = ExcelWorker(path_to_file="test.xlsx")
xls.find_table_from_sheet("2024")
xls.write_to_database("test_table")

