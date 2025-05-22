"""
This is a class for creating data ingestion pipelines.
"""

from class_sql import SQL
from class_delta_lake import DeltaLake
from class_extract_logs import ExtractLogs

class DataIngestion(ExtractLogs):
    def __init__(
        self
        ,sql_server
        ,sql_database
        ,dl_account_name
        ,dl_access_key
        ,sql_username = None
        ,sql_password = None
        ,sql_driver = 'ODBC Driver 18 for SQL Server'
        ,extract_logs_container_name = 'extract-logs' # name of the container where we will be storing extract logs
        ,extract_logs_path = 'extract_logs' # path to the delta table where we will be saving extract logs.
    ):
        super().__init__(
            account_name = dl_account_name
            ,access_key = dl_access_key
            ,container_name = extract_logs_container_name
            ,extract_logs_path = extract_logs_path
        )

        self.sql = SQL(
            server = sql_server
            ,database = sql_database
            ,username = sql_username
            ,password = sql_password
            ,driver = sql_driver
        )
        self.dl = DeltaLake(
            account_name = dl_account_name
            ,access_key = dl_access_key
        )


    def full_load(
        self
        ,source_table_name # name of the source table in a SQL db of the following format: <db_name>.<schema_name>.<table_name>
        ,container_name # name of the container which contains our target table or where we want to create it.
        ,target_table_path # a full path (starting from the root) to the target table in a Data Lake.
        ,if_exists = 'overwrite' # 'overwrite' or 'pass'
    ):
        """
        This function is inserting data into the target delta table in the Data Lake from the entire source table in SQL db.

        The if_exists argument determines what happens when the target table already exist. It can have one of the following values:
            - 'overwrite':  Overwrite the the target table.
            - 'pass':       Don't change the target table at all.
        """

        if if_exists == 'overwrite':
            source_table = self.sql.read_query(f'select * from {source_table_name}')
            self.dl.write_deltalake(source_table, container_name, target_table_path)
        elif if_exists == 'pass' and not self.file_exists(container_name, target_table_path):
            query = f"select * from {source_table_name}"
            source_table = self.sql.read_query(query)
            self.dl.write_deltalake(source_table, container_name, target_table_path)


    def incr_load(
        self
        ,source_table_name # name of the source table in the SQL db of the following format: <db_name>.<schema_name>.<table_name>
        ,container_name # name of the container with the target table
        ,target_table_path # path to the target table in Data Lake container
        ,changes_table_name # name of the changes table in the SQL db of the following format: <db_name>.<schema_name>.<table_name>
        ,change_created_date_column # name of the column in the changes table indicating when the record was created
        ,pk # name of the primary key column in the source table
        ,deleted_col # name of the column in the changes table indicating if given record was deleted in the source table
    ):
        """
        This function is loading data incrementally from the source table in the SQL db into the target delta table in the Data Lake using the changes table.

        The changes table informs us about which records have been added, modified and deleted in the source table and we will use that information in order 
        to update the taget table.

        That changes table needs to have a column indicating if given record has been deleted. 
        So for example if our source table has the following columns:
        
        PK | col1 | col2

        Then the changes table table needs to have columns like that:

        PK | col1 | col2 | deleted

        where PK is a primary key, 'deleted' column indicates if given record has been deleted, and col1 and col2 columns contains
        a new or modified values for that record.
        """

        # if the target table doesn't exist yet, then create it and ingest into it the entire data from the source table.
        # Otherwise don't do anything.
        self.full_load(
            source_table_name = source_table_name
            ,container_name = container_name
            ,target_table_path = target_table_path
            ,if_exists = 'pass'
        )

        # date when the last time we were updating our target table (extracting data)
        last_extract_date = self.find_last_extract_date(target_table_path)

        # load data from the changes table after the last extracted date
        query = f"""
            select
                *
            from
                {changes_table_name}
            where
                {change_created_date_column} > '{last_extract_date}'
        """
        changes_df = self.sql.read_query(query)

        # update the target table in Data Lake using the changes table
        self.dl.update_delta_table(
            changes_df
            ,container_name
            ,target_table_path
            ,pk
            ,deleted_col
        )

        # update the last extracted data in extract logs for the given target table
        self.update_last_extract_date(target_table_path)