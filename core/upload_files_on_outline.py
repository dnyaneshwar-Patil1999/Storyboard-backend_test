import os
import logging
from typing import Optional
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import AzureError
import time


# Load environment variables
load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
NEW_FILESYSTEM_NAME = os.getenv("AZURE_STORAGE_FILESYSTEM_NAME2")
AZURE_STORAGE_FILESYSTEM_NAME = os.getenv("AZURE_STORAGE_FILESYSTEM_NAME")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def _get_datalake_service_client() -> DataLakeServiceClient:
    """
    Returns a DataLakeServiceClient for interacting with ADLS Gen2.
    """
    try:
        account_url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
        service_client = DataLakeServiceClient(account_url=account_url, credential=STORAGE_ACCOUNT_KEY)
        return service_client
    except Exception as e:
        logging.error(f"❌ Failed to initialize DataLakeServiceClient: {e}", exc_info=True)
        raise

CHUNK_SIZE = 4 * 1024 * 1024  # 4 MB per chunk

def normalize_adls_path(*parts: str) -> str:
    """Normalize and join path parts safely for ADLS."""
    clean_parts = [p.strip("/\\") for p in parts if p]
    return "/".join(clean_parts)

def save_source_file_to_adls(
    original_filename: str,
    file_bytes: bytes,
    client: str,
    project: str,
    module: Optional[str] = None
) -> str | None:
    logging.info(f"Attempting to save source file '{original_filename}' to ADLS under client={client}, project={project}, module={module}")

    try:
        datalake_service_client = DataLakeServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
            credential=STORAGE_ACCOUNT_KEY
        )
        file_system_client = datalake_service_client.get_file_system_client(AZURE_STORAGE_FILESYSTEM_NAME)

        # ✅ Normalize directory path
        if not module or module.lower() == "project-level":
            directory_path = normalize_adls_path(client, project)
        else:
            directory_path = normalize_adls_path(client, project, module, "Source")

        logging.info(f"Using directory path: {directory_path}")

        # ✅ Ensure directory exists
        try:
            dir_client = file_system_client.get_directory_client(directory_path)
            dir_client.get_properties()
        except Exception:
            logging.info(f"Creating missing directory path: {directory_path}")
            dir_client = file_system_client.create_directory(directory_path)

        # ✅ Full file path (safe join)
        file_path = normalize_adls_path(directory_path, original_filename)
        logging.info(f"Uploading to ADLS file path: {file_path}")

        file_client = file_system_client.get_file_client(file_path)

        # ✅ Create or overwrite file
        file_client.create_file()
        total_size = len(file_bytes)
        offset = 0
        chunk_size = 4 * 1024 * 1024  # 4 MB

        while offset < total_size:
            chunk = file_bytes[offset:offset + chunk_size]
            retries = 3
            for attempt in range(1, retries + 1):
                try:
                    file_client.append_data(
                        data=chunk,
                        offset=offset,
                        length=len(chunk),
                        timeout=600
                    )
                    break
                except Exception as e:
                    logging.warning(f"Chunk upload failed (attempt {attempt}/{retries}) for {original_filename}: {e}")
                    if attempt == retries:
                        raise
                    time.sleep(3)
            offset += len(chunk)

        file_client.flush_data(total_size, timeout=600)

        constructed_path = f"/{file_path}"
        logging.info(f"✅ SUCCESS: File uploaded to ADLS at {constructed_path}")

        return constructed_path

    except Exception as e:
        logging.error(f"❌ ADLS upload failed for {original_filename}: {e}", exc_info=True)
        return None
