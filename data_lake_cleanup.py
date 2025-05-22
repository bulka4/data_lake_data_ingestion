from pathlib import Path
import os, sys

classes_path = Path(Path(__file__).parent / 'classes').resolve().as_posix()
sys.path.append(classes_path)

from class_azure_blob import AzureBlob

from dotenv import load_dotenv

# name of the container and directory from that container to delete (delete only the directory)
container_names = ['source-data', 'extract-logs']
directory_names = ['source_data', 'extract_logs']

# Load environment variables from .env file
load_dotenv()

account_name = os.getenv('ACCOUNT_NAME')
access_key = os.getenv('ACCESS_KEY')

# Initialize class for working with Data Lake containers, directories and files
blob = AzureBlob(
    account_name
    ,access_key
)

for container_name, directory_name in zip(container_names, directory_names):
    blob.delete_directory(container_name, directory_name)

    print(f'deleted the {directory_name} directory in the {container_name} container.')