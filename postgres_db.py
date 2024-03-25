import psycopg2


class Type:
    INT = "int"
    BOOL = "bool"
    CHAR = "char"
    TEXT = "text"
    FLOAT = "float"
    DATE = "date"


class PostgresDB:
    """Initialize a PostgresDB object.

        Args:
            dbname (str): The name of the database.
            user (str): The username for the database connection.
            password (str): The password for the database connection.

        Returns:
            None"""

    def __init__(self, dbname: str, user: str, password: str) -> None:
        self.connect = psycopg2.connect(
            dbname=dbname, 
            user=user, 
            password=password,
            host="localhost",
            port="5432"
            )
        self.cursor = self.connect.cursor()

    def create_table(self, title: str, columns: dict):
        """
        Create a table in the database with the given title and columns.

        Parameters:
            title (str): The title of the table.
            columns (dict): A dictionary where keys are column names and values are column data types.

        Returns:
            None
        """
        try:        
            self.cursor.execute(f'''CREATE TABLE IF NOT EXISTS {title} ({columns});''')
            self.connect.commit()
            print(f"Table {title} created successfully!")
        except Exception as error:
            print("Something went wrong!")
            print(error)

    def insert_table(self, table_name: str, columns: list, values: list):
        """
        Insert data into a specified table in the database.

        Parameters:
            table_name (str): The name of the table to insert data into.
            columns (list): A list of column names where the data will be inserted.
            values (list): A list of values to be inserted into the table.

        Returns:
            None
        """
        values_for_insert = ''
        for value in values:
            if value is None:
                values_for_insert += 'NULL,'
            elif type(value) in [int, float]:
                values_for_insert += f'{value},'
            elif type(value) == bool:  # noqa: E721
                values_for_insert += 'TRUE,' if value else 'FALSE,'
            else:
                values_for_insert += f'"{value}",'
            values_for_insert = values_for_insert.rstrip(',')
        
        self.cursor.execute(f'''INSERT INTO {table_name} ({", ".join(columns)}) VALUES({values_for_insert});''')
        self.connect.commit()

    def execute_query(self, query: str):
        """
        Execute a query in the database.

        Parameters:
            query (str): The query to execute.

        Returns:
            None
        """
        self.cursor.execute(query)
        self.connect.commit()
    
    def add_column(self, table_name: str, column_name: str, column_type: str):
        """
        Add a column to a table in the database.

        Parameters:
            table_name (str): The name of the table to add the column to.
            column_name (str): The name of the column to add.
            column_type (str): The data type of the column.

        Returns:
            None
        """
        self.cursor.execute(f'''ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS "{column_name}" {column_type};''')
        self.connect.commit()
    
    def drop_table(self, table_name: str):
        """
        Drop a table from the database.

        Parameters:
            table_name (str): The name of the table to drop.

        Returns:
            None
        """
        self.cursor.execute(f'''DROP TABLE IF EXISTS {table_name};''')
        self.connect.commit()

    def drop_column(self, table_name: str, column_name: str):
        """
        Drop a column from a table in the database.

        Parameters:
            table_name (str): The name of the table to drop the column from.
            column_name (str): The name of the column to drop.

        Returns:
            None
        """
        self.cursor.execute(f'''ALTER TABLE {table_name} DROP COLUMN IF EXISTS "{column_name}";''')
        self.connect.commit()

    def set_new_data(self, table_name: str, changes: dict, where: dict):
        """
        Sets new data in the specified table.

        Args:
            table_name (str): The name of the table to update.
            changes (dict): A dictionary containing the changes to be made.
            where (dict): A dictionary containing the conditions for the update.

        Returns:
            None
        """
        changes = ', '.join([f'"{key}" = {value}' for key, value in changes.items()])   # Записываю словарь с данными в нужный вид для программы
        where = ' AND '.join([f'"{key}" = {value}' for key, value in where.items()])    # Записываю словарь с условиями в нужный вид для программы
        self.cursor.execute(f'''UPDATE {table_name} SET {changes} WHERE {where}''')
        self.connect.commit()

    def close(self):
        """
        Close the database connection.

        Args:
            self: The instance of the PostgresDB object.

        Returns:
            None
        """
        self.connect.close()

