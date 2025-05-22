"""
This is a class for working with delta tables (creating, writing data, reading data, updating them incrementally). It extends the AzureBlob class.
"""

from class_azure_blob import AzureBlob

from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import pandas as pd
import json

class DeltaLake(AzureBlob):
    def __init__(
        self
        ,account_name # name of the Azure Storage Account (Data Lake)
        ,access_key # access key to the Azure Storage Account (Data Lake)
    ):
        super().__init__(
            account_name
            ,access_key
        )

        self.account_name = account_name
        self.access_key = access_key


    def write_deltalake(
        self
        ,df: pd.DataFrame # dataframe which we want to save as a delta table
        ,container_name # name of the container where we will save our delta table
        ,path # path where we will save our delta table inside of a given container
        ,mode = 'overwrite' # 'overwrite' or 'append'
    ):
        """
        Function for saving a dataframe as a delta table in Data Lake.
        The mode argument indicates what happens if the table already exists at the specified path.
        """
        
        storage_options = {
            "account_name": self.account_name
            ,"access_key": self.access_key
        }
        
        write_deltalake(
            f'abfss://{container_name}@{self.account_name}.dfs.core.windows.net/{path}'
            ,df
            ,storage_options = storage_options
            ,mode = mode
        )

    
    def read_deltalake(
        self
        ,container_name # name of the container where our delta table is saved from which we will read the data.
        ,path # path to the delta table from which we will read the data.
        ,to_pandas = False # if to_pandas = True then it will return a pandas.DataFrame instead of DeltaTable.
    ):
        """
        Read data from the delta table in Data Lake.
        """
        if not self.file_exists(container_name, path):
            raise Exception("Table doesn't exist")

        storage_options = {
            "account_name": self.account_name
            ,"access_key": self.access_key
        }
        
        delta_table = DeltaTable(
            f'abfss://{container_name}@{self.account_name}.dfs.core.windows.net/{path}'
            ,storage_options = storage_options
        )

        if to_pandas:
            return delta_table.to_pandas()
        else:
            return delta_table

    
    def delta_table_columns(self, delta_table: DeltaTable):
        """
        This function returns a list of column names for a given delta table.
        """
        
        fields = json.loads(delta_table.schema().to_json())['fields']
        columns = [field['name'] for field in fields]
        
        return columns


    def update_delta_table(
        self
        ,changes_df: pd.DataFrame  # changes table which contains data about what changes happened to the source table
        ,container_name # name of the container which contains the table which we want to update
        ,target_table_path # path to the target delta table which will be updated based on the changes_df table
        ,pk # name of the primary key
        ,deleted_col # name of the column from the changes_df table indicating if given record was deleted in the source table
    ):
        """
        This function is incrementally ingesting data from the source table into the target one using the changes table.
        
        The changes table informs us about which records have been added, modified and deleted in the source table and we will use that 
        information in order to update the taget table.

        changes_df argument needs to have a column indicating if given record has been deleted. 
        So for example if our source table has the following columns:
        
        PK | col1 | col2

        Then the changes_df table needs to have columns like that:

        PK | col1 | col2 | deleted

        where PK is a primary key, 'deleted' column indicates if given record has been deleted, and col1 and col2 columns contains
        a new or modified values for that record.
        """
        
        target_dt = self.read_deltalake(container_name, target_table_path)
        target_dt_columns = self.delta_table_columns(target_dt)

        # update records in the target table which were modified at the source
        (
            target_dt.merge(
                source = changes_df
                ,predicate = f"""
                    target.{pk} = source.{pk}
                    and source.{deleted_col} = 0
                """
                ,source_alias = "source"
                ,target_alias = "target"
            )
            .when_matched_update(
                updates = {col: f'source.{col}' for col in target_dt_columns}
            )
            .execute()
        )

        # insert into the target table new records from the source
        (
            target_dt.merge(
                source = changes_df
                ,predicate = f"""
                    target.{pk} = source.{pk}
                """
                ,source_alias = "source"
                ,target_alias = "target"
            )
            .when_not_matched_insert(
                updates = {col: f'source.{col}' for col in target_dt_columns}
            )
            .execute()
        )

        # delete records from the target table which were deleted at the source
        (
            target_dt.merge(
                source = changes_df
                ,predicate = f"""
                    target.{pk} = source.{pk}
                """
                ,source_alias = "source"
                ,target_alias = "target"
            )
            .when_matched_delete(
                predicate = f'source.{deleted_col} = 1'
            )
            .execute()
        )