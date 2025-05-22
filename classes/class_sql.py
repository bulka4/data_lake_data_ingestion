"""
This is a class for working with SQL. This class is used in the DataIngestion class.

That script also works with newer libraries:
SQLAlchemy==2.0.41
pandas==2.2.3
numpy==2.2.5
"""

import pandas as pd
import sqlalchemy as sa

class SQL:
    def __init__(
        self
        ,server
        ,database
        ,username = None
        ,password = None
        ,driver = 'ODBC Driver 18 for SQL Server'
    ):
        
        if username == None and password == None:
            connection_url = f'mssql://@{server}/{database}?driver={driver}'
        else:
            connection_url = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}"
        
        self.engine = sa.create_engine(connection_url, fast_executemany=True)
        
    def read_query(self, query):
        "saving a result of a sql query in a dataframe"
        
        with self.engine.connect() as con:
            df = pd.read_sql(sql = sa.text(query), con = con)
        
        return df
    
    def read_sql_file(self, file_path):
        "saving a result of a sql query from a file to a dataframe"
        
        with open(file_path, 'r') as query:
            with self.engine.connect() as con:
                df = pd.read_sql(sql = sa.text(query.read()), con = con)
                
        return df
        
    def execute_sql_file(self, file_path):
        "executing sql file"
        
        with open(file_path, 'r') as file:
            con = self.engine.raw_connection()
            with con.cursor() as cursor:
                cursor.execute(file.read())
            
            con.commit()
            con.close()
            
    def execute_query(self, query):
        "executing sql query"
        con = self.engine.raw_connection()
        with con.cursor() as cursor:
            cursor.execute(query)

        con.commit()
        con.close()
    
    def to_sql(
        self
        ,dataframe: pd.DataFrame # dataframe from which data will be ingested into the SQL table.
        ,sql_table_name # name of the SQL table into which we will insert data.
        ,sql_schema_name # name of the schema of the SQL table into which we will insert data.
        ,if_exists # 'append' or 'replace'
    ):
        """
        Inserting data from a dataframe into a sql database.
        
        Argument if_exists indicates what happens if table in SQL db given by the sql_table_name argument already exists.
        Possible values for that argument are: 'append' or 'replace'.
        """
        
        col_count = len(dataframe.columns)
        max_params = 1000
        chunksize =  max_params // col_count
        
        
        # if schema doesnt exist then create it
        with self.engine.raw_connection().cursor() as cursor:
            cursor.execute(f"""IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{sql_schema_name}') 
            exec ('CREATE SCHEMA {sql_schema_name}')""")

        with self.engine.connect() as con:
            dataframe.to_sql(
                name = sql_table_name
                ,schema = sql_schema_name
                ,con = con
                ,index = False
                ,if_exists = if_exists
                ,chunksize = chunksize
                ,method = "multi"
            )