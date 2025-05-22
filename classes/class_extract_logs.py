"""
This is a class for working with extract logs which are used for incremental load. This class is a parent to the DataIngestion class.

Extract logs contains information about when the last time data was ingested from the source into the target table. It is used to determine
which records needs to be ingested into the target table.
"""

from class_azure_blob import AzureBlob
from class_delta_lake import DeltaLake

import pandas as pd
import os
from datetime import datetime

class ExtractLogs(AzureBlob):
    def __init__(
        self
        ,account_name # name of the Azure Storage Account (Data Lake)
        ,access_key # access key to the Azure Storage Account (Data Lake)
        ,container_name = 'extract-logs' # name of the container where we will be storing extract logs
        ,extract_logs_path = 'extract_logs' # a full path (starting from the root) to the delta table where we will be saving extract logs.
    ):
        super().__init__(
            account_name
            ,access_key
        )

        self.dl = DeltaLake(
            account_name
            ,access_key
        )

        self.extract_logs_path = extract_logs_path
        self.container_name = container_name

        if container_name not in self.list_containers():
            self.create_container(container_name)

        self.load_extract_logs()


    def load_extract_logs(
        self
    ):
        """
        Load information from the extract log delta table about when the last time data for every table was extracted.
        Class parameters specifies where that extract log delta table is located. 
        
        If this file doesn't exist yet then this function creates an empty dataframe assigned to the self.extract_logs attribute.
        """
        # check if we don't have the extract logs delta table created in the Data Lake yet
        if not self.file_exists(self.container_name, self.extract_logs_path):
            self.extract_logs = pd.DataFrame(columns = ['table_path', 'last_extract_date'])
        else:
            # load extract logs from the extract logs delta table in the Data Lake
            self.extract_logs = self.dl.read_deltalake(self.container_name, self.extract_logs_path, to_pandas = True)


    def save_extract_logs(self):
        """
        This function is saving the extract logs in the delta table in the Data Lake. Location of that table
        is specified by the class parameters.
        """
        self.dl.write_deltalake(self.extract_logs, self.container_name, self.extract_logs_path)


    def find_last_extract_date(self, table_path):
        """
        Find the last extract date for a given table path in the extract logs.

        If there is no record for a given table path in the extract logs then this function
        returns '1900-01-01,00-00-00' string.
        """
        indexes = self.extract_logs[self.extract_logs.table_path == table_path].index
        
        if len(indexes) == 0:
            return '1900-01-01,00-00-00'
        else:
            last_extract_date = self.extract_logs.loc[indexes[0], 'last_extract_date']

        return last_extract_date


    def update_last_extract_date(self, table_path):
        """
        Update the last extract date for a given table path in the self.extract_logs dataframe and save
        logs in the Data Lake. If there is no record for that table yet then create it.
        """
        
        indexes = self.extract_logs[self.extract_logs.table_path == table_path].index
        
        if len(indexes) == 0:
            self.extract_logs = pd.concat((
                self.extract_logs
                ,pd.DataFrame(
                    [[table_path, datetime.utcnow().strftime('%Y-%m-%d,%H-%M-%S')]]
                    ,columns = ['table_path', 'last_extract_date']
                )
            ))
        else:
            self.extract_logs.loc[indexes[0], 'last_extract_date'] = datetime.utcnow().strftime('%Y-%m-%d,%H-%M-%S')

        # save extract logs in the Data Lake
        self.save_extract_logs()