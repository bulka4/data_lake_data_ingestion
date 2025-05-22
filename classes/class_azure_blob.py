"""
This is a class for working with containers, directories and files (creating them, deleting, renaming). This class is a parent to the DeltaLake and ExtractLogs classes.
"""

from azure.storage.blob import generate_account_sas, ResourceTypes, AccountSasPermissions
from azure.storage.filedatalake import DataLakeServiceClient
import azure.core.exceptions

from datetime import datetime, timedelta
import numpy as np

class AzureBlob:
    def __init__(
        self
        ,account_name # name of the Azure Storage Account (Data Lake)
        ,access_key # access key to the Azure Storage Account (Data Lake)
    ):
        self.account_name = account_name
        self.access_key = access_key
        # Create a service client which will be used for performing all the operations on containers, directories and files.
        self.create_service_client()


    def create_service_client(
        self
    ):
        """
        This function creates a service client if it was not created yet. It will be used for performing all the operations on containers, directories and files.

        It generates at first a SAS token if it was not generated yet or if it expired.
        """
        if (
            not hasattr(self, 'sas_expiry_date')
        ) or (
            hasattr(self, 'sas_expiry_date') and self.sas_expiry_date < datetime.utcnow()
        ):
            sas = self.create_account_sas()

            self.service_client = DataLakeServiceClient(
                account_url = f'https://{self.account_name}.dfs.core.windows.net'
                ,credential = sas
            )


    def create_account_sas(
        self
    ):
        """
        Function for creating an account SAS token (This is needed for authentication when connecting to Azure Blob Storage).
        """
        # Define the start and expiry time for the SAS 
        start_time = datetime.utcnow()
        self.sas_expiry_date = start_time + timedelta(
            # hours = 1
            minutes = 10
            # seconds = 5
        )
    
        # Define the SAS permissions
        sas_permissions = AccountSasPermissions(read=True, write=True, delete=True, list=True)
    
        # Define the SAS resource types
        sas_resource_types = ResourceTypes(service=True, container=True, object=True)

        # Generate the SAS
        sas = generate_account_sas(
            account_name = self.account_name
            ,account_key = self.access_key
            ,resource_types = sas_resource_types
            ,permission = sas_permissions
            ,expiry = self.sas_expiry_date
            ,start = start_time
        )

        return sas
        

    def create_container(
        self
        ,container_name
    ):
        self.create_service_client()
        self.service_client.create_file_system(container_name)


    def delete_container(
        self
        ,container_name
    ):
        self.create_service_client()
        self.service_client.delete_file_system(container_name)


    def list_containers(
        self
    ):
        """
        Returns names of all the containers
        """
        self.create_service_client()
        file_systems = self.service_client.list_file_systems()
        
        return [file_system.name for file_system in file_systems]            


    def create_directory(
        self
        ,container_name
        ,directory_name
    ):
        self.create_service_client()
        file_system_client = self.service_client.get_file_system_client(container_name)
        file_system_client.create_directory(directory_name)


    def delete_directory(
        self
        ,container_name
        ,directory_name
    ):
        self.create_service_client()
        file_system_client = self.service_client.get_file_system_client(container_name)
        file_system_client.delete_directory(directory_name)

    
    def rename_directory(
        self
        ,container_name
        ,directory_name
        ,new_directory_name
    ):
        self.create_service_client()
        directory_client = self.service_client.get_directory_client(container_name, directory_name)
        directory_client.rename_directory(new_name = f"{container_name}/{new_directory_name}")

    
    def upload_file(
        self
        ,container_name
        ,cloud_file_path
        ,local_file_path
    ):
        self.create_service_client()
        file_client = self.service_client.get_file_client(container_name, cloud_file_path)

        with open(file = local_file_path, mode = "rb") as data:
            file_client.upload_data(data, overwrite = True)

    
    def list_directory_content(
        self
        ,container_name
        ,path # a full path (starting from the root, it doesn't matter if we include the '/' at the beginning) to the directory which content we want to see.
    ):
        """
        This function lists all the files and directories which are contained in the directory given by the 'path' argument. 
        It returns full paths of the files inside of the given directory.

        For example if path = 'directory1' then it might return: 'directory1/file1.csv', 'directory1/directory2', 'directory1/directory2/file2.csv'.
        If given directory doesn't exist then it will raise an exception.
        """
        self.create_service_client()
        file_system_client = self.service_client.get_file_system_client(container_name)
        try:
            paths = file_system_client.get_paths(path = path)
            paths = np.array([path.name for path in paths])
        except azure.core.exceptions.ResourceNotFoundError:
            raise Exception("Specified directory doesn't exist")

        return paths


    def file_exists(
        self
        ,container_name
        ,file_path # a full path (starting from the root, it doesn't matter if we include the '/' at the beginning) to the file which we want to check.
    ) -> bool:
        """
        This function checks if the file (or directory) at the specified path exists in the given container.
        """
        self.create_service_client()
        file_client = self.service_client.get_file_client(container_name, file_path)
        try:
            file_client.get_file_properties()
            return True
        except azure.core.exceptions.ResourceNotFoundError:
            return False