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

        self.df = pd.read_excel(self.path_to_file, sheet_name=sheet_name, header=0)
        for column in self.df.columns: # Remove empty columns from self.df.columns:
            empty_cells_col = 0
            for value in self.df[column]:
                if pd.isna(value):
                    empty_cells_col += 1
            if empty_cells_col == len(self.df[column]):
                self.df = self.df.drop(column, axis=1)

    def guess_types_of_columns(self):  # sourcery skip: merge-else-if-into-elif
        column_types_dict = {}

        for column in self.df.columns:
            current_column_types = []
            for value in self.df[column]:
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
                column_types_dict[f"{column}"] = Type.FLOAT
            elif "int" in current_column_types:
                column_types_dict[f"{column}"] = Type.INT
            elif "text" in current_column_types:
                column_types_dict[f"{column}"] = Type.TEXT
            elif "char" in current_column_types:
                column_types_dict[f"{column}"] = Type.CHAR
            elif "bool" in current_column_types:
                column_types_dict[f"{column}"] = Type.BOOL
            elif "date" in current_column_types:
                column_types_dict[f"{column}"] = Type.DATE
            else:
                column_types_dict[f"{column}"] = Type.TEXT
        
        column_types = ', '.join([f'"{key}" {value}' for key, value in column_types_dict.items()])
        column_titles = [f'"{key}"' for key in column_types_dict]

        return column_types, column_titles 


    def write_to_database(self, table_name: str):
        column_types, column_titles = self.guess_types_of_columns()
        psql = PostgresDB(dbname="test_database", user=s.POSTGRES_USER, password=s.POSTGRES_PASSWORD)
        psql.create_table(title=table_name, columns=column_types)
        for row in self.df.iterrows():
            current_row = [row[1][i] for i in range(len(row[1]))]
            psql.insert_table(table_name=table_name, columns=column_titles, values=self.make_values_tuple(current_row))
        

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

    @staticmethod
    def make_values_tuple(row: list):
        """
        Create a tuple of values from the given dictionary row. 
        Takes a dictionary row as input and returns a string of values.
        """
        values = ""
        for value in row:
            if value is None:
                values += "NULL,"
            elif type(value) in [int, float]:
                values += f"{value},"
            elif type(value) == bool:  # noqa: E721
                values += "TRUE," if value else "FALSE,"
            else:
                values += f'"{value}",'
        return values.rstrip(",")

xls = ExcelWorker(path_to_file="test.xlsx")
xls.find_table_from_sheet("2024")
xls.write_to_database("test_table")

