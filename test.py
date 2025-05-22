from pathlib import Path
import os, sys

classes_path = Path(Path(__file__).parent / 'classes').resolve().as_posix()
sys.path.append(classes_path)

from class_sql import SQL
from class_azure_blob import AzureBlob
from class_delta_lake import DeltaLake

from dotenv import load_dotenv
import pandas as pd
import numpy as np

# Load environment variables from .env file
load_dotenv()

account_name = os.getenv('ACCOUNT_NAME')
access_key = os.getenv('ACCESS_KEY')

server_name = os.getenv('SQL_SERVER_NAME')
database = os.getenv('SQL_DB_NAME')
sql_username = os.getenv('SQL_USERNAME')
sql_password = os.getenv('SQL_PASSWORD')

def test_sql():
    sql = SQL(
        server = server_name
        ,database = database
        ,username = sql_username
        ,password = sql_password
    )

    df = sql.read_query('select * from db.fact.table2')

    print(f'\ntable2 in SQL: \n{df}')

    # sql.execute_query('select * from db.fact.table2')
    # sql.to_sql(
    #     dataframe = df
    #     ,sql_table_name = 'table2'
    #     ,sql_schema_name = 'fact'
    #     ,if_exists = 'replace'
    # )


def test_data_lake():
    dl = DeltaLake(
        account_name = account_name
        ,access_key = access_key
    )

    df = dl.read_deltalake(
        container_name = 'source-data'
        ,path = 'source_data/table2'
        ,to_pandas = True
    )

    print(f'\ntable2 in Data Lake: \n{df}')


def test_blob():
    blob =  AzureBlob(
        account_name = account_name
        ,access_key = access_key
    )

    container_name = 'source-data'
    # path = '/source_date/file'
    path = 'source_data/table2'

    print(blob.list_directory_content(container_name, path))

    # print(blob.file_exists(container_name, path))


test_sql()
test_data_lake()