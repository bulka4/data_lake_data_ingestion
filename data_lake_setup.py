"""
This script is creating a specified container and directory where we will be ingesting data.
"""

from pathlib import Path
import os, sys

classes_path = Path(Path(__file__).parent / 'classes').resolve().as_posix()
sys.path.append(classes_path)

from class_azure_blob import AzureBlob
from config import container_name, directory_name # name of the container and directory in that container where we will be ingesting data.
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

account_name = os.getenv('ACCOUNT_NAME')
access_key = os.getenv('ACCESS_KEY')

# Initialize class for working with Data Lake containers, directories and files
blob = AzureBlob(
    account_name
    ,access_key
)

# create container source_data and folder source_data in that container if they don't exist yet.
if container_name not in blob.list_containers():
    blob.create_container(container_name)
if not blob.file_exists(container_name, directory_name):
    blob.create_directory(container_name, directory_name)