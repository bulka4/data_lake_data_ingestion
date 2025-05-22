"""
This script is creating tables in a SQL db. At the beginning of that script we are creating the sql_tables variable which specifies what tables will be created.

We need to create the .env file and provide there values for all the variables which are accessed in this script using the os.getenv() function.
"""

from pathlib import Path
import os, sys

classes_path = Path(Path(__file__).parent / 'classes').resolve().as_posix()
sys.path.append(classes_path)

from class_sql import SQL

from dotenv import load_dotenv
import pandas as pd
import numpy as np
from datetime import datetime

now_date = datetime.utcnow().strftime('%Y-%m-%d,%H-%M-%S')

# Define tables which we will create in the SQL db. The sql_tables dictionary has the following format:
# [
#     {
#         'table_name': 'table1'
#         ,'table_schema': 'table1_schema'
#         ,'data': pd.DataFrame
#     }
#     ,{
#         'table_name': 'table2'
#         ,'table_schema': 'table2_schema'
#         ,'data': pd.DataFrame
#     }
#     ,...
# ]
sql_tables = [
    # table for incremental load
    {
        'table_name': 'table2'
        ,'table_schema': 'fact'
        ,'data': pd.DataFrame(
            [
                [2, 22, 22]
                ,[3, 3, 3]
            ]
            ,columns = ['ID', 'dim_col', 'measure']
        )
    }
    # changes table for incremental load. The 'deleted' column indicates if given record has been deleted from the table.
    # The 'date_created' column indicates when that record has been created in the changes table.
    ,{
        'table_name': 'table2_changes'
        ,'table_schema': 'fact'
        ,'data': pd.DataFrame(
            [
                [1, 1, 1, 1, now_date] # deleted record to test incremental load
                ,[2, 22, 22, 0, now_date] # modified record to test incremental load
                ,[3, 3, 3, 0, now_date] # new record added to test incremental load
            ]
            ,columns = ['ID', 'dim_col', 'measure', 'deleted', 'date_created']
        )
    }
]

# Load environment variables from .env file
load_dotenv()

server_name = os.getenv('SQL_SERVER_NAME')
database = os.getenv('SQL_DB_NAME')
sql_username = os.getenv('SQL_USERNAME')
sql_password = os.getenv('SQL_PASSWORD')

sql = SQL(
    server = server_name
    ,database = database
    ,username = sql_username
    ,password = sql_password
)

# we will replace the first table (table2) and append new records to the second table (table2_changes)
for sql_table, if_exist in zip(sql_tables, ['replace', 'append']):
    sql.to_sql(
        dataframe = sql_table['data']
        ,sql_table_name = sql_table['table_name']
        ,sql_schema_name = sql_table['table_schema']
        ,if_exists = if_exist
    )