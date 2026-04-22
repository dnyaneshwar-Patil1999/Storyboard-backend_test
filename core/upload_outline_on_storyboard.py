import os
import logging
from typing import Optional # Added for type hinting
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import AzureError, ResourceNotFoundError

load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
FILESYSTEM_NAME = os.getenv("AZURE_STORAGE_FILESYSTEM_NAME")
# -----------------------------------------

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ALLOWED_EXTENSIONS = {'.docx'}
# Added allowed extensions for source files as indicated in the frontend
ALLOWED_SOURCE_EXTENSIONS = {'.pdf', '.doc', '.docx', '.txt'}


def save_uploaded_outline_to_adls(
    original_filename: str,
    file_bytes: bytes,
    client: str,
    project: str,
    module: str
) -> str | None:
    """
    Validates and saves a DOCX file to a dynamic path in ADLS.
    """
    logging.info(f"Attempting to save '{original_filename}' to ADLS path: {client}/{project}/{module}")
    try:
        # --- Stage 1: Validation ---
        _base, extension = os.path.splitext(original_filename)
        if extension.lower() not in ALLOWED_EXTENSIONS:
            raise ValueError(f"Invalid file type: '{extension}'. Only DOCX is allowed.")

        # --- Stage 2: ADLS Connection ---
        account_url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
        service_client = DataLakeServiceClient(account_url=account_url, credential=STORAGE_ACCOUNT_KEY)
        file_system_client = service_client.get_file_system_client(file_system=FILESYSTEM_NAME)
        directory_path = f"{client}/{project}/{module}/Outline/"
        
        # --- Stage 3: Ensure Directory Exists ---
        try:
            file_system_client.create_directory(directory_path)
        except AzureError as e:
            if e.error_code != 'PathAlreadyExists': raise

        # --- Stage 4: Find Unique Filename ---
        base_filename, extension = os.path.splitext(original_filename)
        counter = 0
        while True:
            current_filename = f"{base_filename}{counter if counter > 0 else ''}{extension}"
            final_adls_path = f"{directory_path}{current_filename}"
            file_client = file_system_client.get_file_client(final_adls_path)
            if not file_client.exists():
                break
            counter += 1
        
        # --- Stage 5: Upload Data ---
        upload_file_client = file_system_client.get_file_client(final_adls_path)
        upload_file_client.upload_data(file_bytes, overwrite=True)
        
        logging.info(f"✅ SUCCESS: File saved to {final_adls_path}")
        return final_adls_path

    except (ValueError, AzureError) as e:
        logging.error(f"❌ UPLOAD FAILED for {original_filename}: {e}", exc_info=False)
        return None
    except Exception as e:
        logging.error(f"❌ An unexpected critical error occurred during upload of {original_filename}", exc_info=True)
        return None


def get_outlines_from_adls(client: str, project: str, module: str) -> list[str]:
    """
    Lists all files inside a specific ADLS directory: client/project/module/Outline/
    Returns an empty list if the directory does not exist or an error occurs.
    """
    directory_path = f"{client}/{project}/{module}/Outline/"
    logging.info(f"Fetching outlines from ADLS directory: {directory_path}")
    
    try:
        account_url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
        service_client = DataLakeServiceClient(account_url=account_url, credential=STORAGE_ACCOUNT_KEY)
        file_system_client = service_client.get_file_system_client(file_system=FILESYSTEM_NAME)
        
        # Get directory contents
        path_generator = file_system_client.get_paths(path=directory_path)
        
        outline_names = [
            os.path.basename(path.name)
            for path in path_generator
            if not path.is_directory
        ]
        
        logging.info(f"Found {len(outline_names)} outlines in '{directory_path}'.")
        return sorted(outline_names) # Sort alphabetically for consistent ordering

    except ResourceNotFoundError:
        logging.warning(f"Directory not found, returning empty list: {directory_path}")
        return [] # This is an expected case, not an error.
    except Exception as e:
        logging.error(f"Failed to list outlines from ADLS: {e}", exc_info=True)
        return [] # Return an empty list on any other failure.
    