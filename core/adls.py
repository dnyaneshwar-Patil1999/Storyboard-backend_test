import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    FileSasPermissions,
    generate_file_sas
)
load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
FILESYSTEM_NAME = os.getenv("AZURE_STORAGE_FILESYSTEM_NAME")
CHUNKS_CONTAINER_NAME = os.getenv("STORAGE_CONTAINER_NAME")
 
def get_adls_client():
    account_url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=STORAGE_ACCOUNT_KEY)
 
def build_tree(paths):
    """Helper function to build a nested dictionary from file paths."""
    tree = {}
    for path in paths:
        parts = path.name.split("/")
        current = tree
        for part in parts:
            if part not in current:
                current[part] = {}
            current = current[part]
    return tree
 
def get_file_structure(container_name: str= "aptara"):
    """Fetches all file paths from a specific ADLS container and builds folder tree."""
    try:
        service_client = get_adls_client()
        fs_client = service_client.get_file_system_client(container_name)
        paths = list(fs_client.get_paths(path="", recursive=True))
        tree = build_tree(paths)
        return tree
    except Exception as e:
        print(f"❌ Error fetching ADLS structure for container '{container_name}': {e}")
        return {}



def create_sas_url(file_system_name: str, file_path: str) -> str | None:
    """Generates a read-only SAS URL for a specific file, valid for 15 minutes."""
    try:
        directory = os.path.dirname(file_path)
        file_name = os.path.basename(file_path)
 
        sas_token = generate_file_sas(
            account_name=STORAGE_ACCOUNT_NAME,
            file_system_name=file_system_name,
            directory_name=directory,
            file_name=file_name,
            credential=STORAGE_ACCOUNT_KEY,
            # Define permissions: The user can only READ the file.
            permission=FileSasPermissions(read=True),
            # Define expiry time: The link will stop working after 15 minutes.
            expiry=datetime.utcnow() + timedelta(minutes=15),
        )
        
        # Construct the full, usable URL for the browser.
        return f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{file_system_name}/{file_path}?{sas_token}"
 
    except Exception as e:
        print(f"Error generating SAS token for '{file_path}': {e}")
        return None
    