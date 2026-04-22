from fastapi import APIRouter, Body, HTTPException, Query, status
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
from typing import Any, Dict, List, Optional, Union

import io
import os
import json
import base64
import logging
import re

from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_ALIGN_VERTICAL
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.shared import Inches, Pt
from docx.oxml import OxmlElement, ns
from docx.image.exceptions import UnexpectedEndOfFileError
from docx.document import Document as DocumentType
from docx.table import _Cell

from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import AzureError, ResourceNotFoundError
from bs4 import BeautifulSoup

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
AZURE_STORAGE_FILESYSTEM_NAME = os.getenv("AZURE_STORAGE_FILESYSTEM_NAME")


def get_adls_client():
    if not all([AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_ACCOUNT_KEY]):
        raise ValueError("Azure Storage Account Name and Key must be configured in environment variables.")
    account_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=AZURE_STORAGE_ACCOUNT_KEY)

def get_latest_outline_stream(client: str, project: str, module: str) -> tuple[io.BytesIO, str]:
    """Gets the latest outline file stream from ADLS."""
    service_client = get_adls_client()
    file_system_client = service_client.get_file_system_client(file_system=AZURE_STORAGE_FILESYSTEM_NAME)
    outline_path = f"{client}/{project}/{module}/Outline"
    paths = list(file_system_client.get_paths(path=outline_path))
    outlines = [p for p in paths if not p.is_directory]
    if not outlines:
        raise Exception(f"No outline files found in path: {outline_path}")
    latest_file = max(outlines, key=lambda x: x.last_modified)
    file_client = file_system_client.get_file_client(latest_file.name)
    download = file_client.download_file()
    file_content = download.readall()
    # Get the original filename without any cleaning
    original_filename = latest_file.name.split('/')[-1]
    # Return the original filename as it is in Azure Storage
    return io.BytesIO(file_content), original_filename

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
 
 
 
 
 
 
 
 
def download_file_from_adls(client: str, project: str, module: str, filename: str) -> bytes | None:
    """
    Download the latest version of a file from Azure Data Lake Storage (ADLS).
    Handles versioned filenames like file.docx, file(1).docx, file(2).docx, etc.
    Always picks the latest one based on last_modified timestamp.
    """
    file_path = f"{client}/{project}/{module}/Outline/{filename}"
    logger.info(f"Attempting to download latest version of file from ADLS path: {file_path}")

    try:
        service_client = get_adls_client()
        file_system_client = service_client.get_file_system_client(file_system=AZURE_STORAGE_FILESYSTEM_NAME)

        # Determine correct directory
        if module and module != "project-level":
            outline_dir = f"{client}/{project}/{module}/Outline"
        else:
            outline_dir = f"{client}/{project}/Outline"

        # List all files in the Outline directory
        paths = list(file_system_client.get_paths(path=outline_dir))
        available_files = [p.name.split('/')[-1] for p in paths if not p.is_directory]
        logger.info(f"Available files in '{outline_dir}': {available_files}")

        normalized_filename_to_find = filename.strip().lower()
        base, ext = os.path.splitext(normalized_filename_to_find)

        # Find all files that match the base name (with or without version suffix)
        matching_files = []
        for path in paths:
            if not path.is_directory:
                adls_filename = path.name.split('/')[-1]
                normalized_adls_filename = adls_filename.strip().lower()
                if normalized_adls_filename == normalized_filename_to_find or \
                   (normalized_adls_filename.startswith(base) and normalized_adls_filename.endswith(ext)):
                    matching_files.append((path, adls_filename))

        if matching_files:
            # Pick the latest file by modification time
            latest_path, latest_filename = max(matching_files, key=lambda x: x[0].last_modified)
            logger.info(f"Found latest version: '{latest_filename}' (Modified: {latest_path.last_modified})")

            match_file_client = file_system_client.get_file_client(latest_path.name)
            download = match_file_client.download_file()
            return download.readall()

        logger.error(f"No matching files found for '{filename}' in '{outline_dir}'.")
        return None

    except ResourceNotFoundError:
        logger.warning(f"Resource not found for path: {file_path}")
        return None
    except AzureError as azure_err:
        logger.error(f"Azure error while downloading file '{file_path}': {azure_err}")
        return None
    except Exception as e:
        logger.exception(f"Unexpected error while downloading file '{file_path}': {e}")
        return None


def parse_outline_metadata(file_content: bytes) -> Dict[str, Any]:
    """
    Parse DOCX file content to extract specific metadata fields such as User Prompt and Topic.
 
    Args:
        file_content (bytes): The binary content of a DOCX file.
 
    Returns:
        Dict[str, Any]: A dictionary containing:
            - user_prompt (str): Extracted user prompt text (if found).
            - topic (str): Extracted topic text (if found).
 
    Raises:
        ValueError: If the file content is empty or invalid.
    """
    if not file_content:
        raise ValueError("File content is empty. Cannot parse metadata.")
 
    metadata: Dict[str, str] = {
        "user_prompt": "",
        "topic": ""
    }
 
    try:
        # Create a file-like object from bytes and load DOCX
        docx_file = io.BytesIO(file_content)
        document = Document(docx_file)
 
        # Extract all non-empty paragraphs
        full_text = [p.text.strip() for p in document.paragraphs if p.text.strip()]
        combined_text = "\n".join(full_text)
 
        # Extract sections with improved logic
        metadata["user_prompt"] = extract_section_improved(combined_text, "User Prompt") or \
                                 find_pattern_content(combined_text, ["User Prompt:", "Prompt:", "Prompt"])
       
        metadata["topic"] = extract_section_improved(combined_text, "Topic") or \
                           find_pattern_content(combined_text, ["Topic:", "Subject:", "Topic", "Subject"])
 
        return metadata
 
    except Exception as e:
        logger.error("Error parsing DOCX content: %s", e, exc_info=True)
        return metadata
 

def extract_section_improved(text: str, section_name: str) -> str:
    """
    Extracts the section content after a section header, stopping at the next header.
    Specifically designed for extracting user prompt content.
    """
    try:
        lines = text.split('\n')
        section_content = []
        in_section = False
        found_section = False
       
        for i, line in enumerate(lines):
            line_stripped = line.strip()
           
            # Check if this line is the section header we're looking for
            if (line_stripped.lower() == section_name.lower() or
                line_stripped.lower().startswith(section_name.lower())):
                if not found_section:
                    found_section = True
                    in_section = True
                    # Skip the header line itself
                    continue
           
            # If we're in the section, collect content lines
            if in_section:
                # Stop when we hit the next section header (lines starting with "##" or similar)
                if (line_stripped.startswith('##') or
                    line_stripped.lower() == 'topic' or
                    (i + 1 < len(lines) and lines[i + 1].strip().startswith('##'))):
                    break
               
                # Only add non-empty lines that are not section headers
                if line_stripped and not line_stripped.startswith('##'):
                    section_content.append(line_stripped)
       
        # Return all collected lines joined with newline
        result = "\n".join(section_content) if section_content else ""
        logger.debug(f"Extracted section '{section_name}': {result}")
        return result
       
    except Exception as e:
        logger.error(f"Error extracting section '{section_name}': {e}")
        return ""
 
 
 
def find_pattern_content(text: str, patterns: List[str]) -> str:
    """
    Finds content in the text following specific patterns.
 
    Args:
        text (str): The complete text to search within.
        patterns (List[str]): A list of patterns to look for.
 
    Returns:
        str: The content following the first matched pattern.
             Returns an empty string if no pattern matches.
    """
    try:
        lines = text.split('\n')
 
        for i, line in enumerate(lines):
            line_lower = line.lower()
           
            for pattern in patterns:
                pattern_lower = pattern.lower()
               
                # Check if pattern exists in this line
                if pattern_lower in line_lower:
                    # If line contains only the pattern (or pattern with colon), get next line
                    if (line_lower.strip() == pattern_lower or
                        line_lower.strip() == pattern_lower + ':'):
                        # Look for the next non-empty line
                        for j in range(i + 1, len(lines)):
                            next_line = lines[j].strip()
                            if next_line:
                                # Make sure next line isn't another section header
                                if not is_likely_header(next_line):
                                    logger.debug(f"Found pattern '{pattern}' content in next line: {next_line}")
                                    return next_line
                                break
                   
                    # If pattern is part of the line with content
                    else:
                        # Handle colon-separated patterns
                        if ':' in line:
                            parts = line.split(':', 1)
                            if len(parts) > 1 and pattern_lower in parts[0].lower():
                                content = parts[1].strip()
                                if content:
                                    logger.debug(f"Found pattern '{pattern}' with colon: {content}")
                                    return content
                       
                        # Handle pattern followed by content
                        pattern_index = line_lower.find(pattern_lower)
                        if pattern_index != -1:
                            content_start = pattern_index + len(pattern)
                            content = line[content_start:].strip()
                            if content:
                                # Remove any leading punctuation
                                if content.startswith((':', '-', '—')):
                                    content = content[1:].strip()
                                logger.debug(f"Found pattern '{pattern}' inline content: {content}")
                                return content
 
        return ""
 
    except Exception as e:
        logger.error(f"Error finding pattern content: {e}")
        return ""
 
 
def is_likely_header(line: str) -> bool:
    """
    Simple check if a line is likely a section header.
   
    Args:
        line (str): The line to check.
       
    Returns:
        bool: True if the line looks like a header.
    """
    line_lower = line.lower()
   
    # Common header indicators
    header_indicators = [
        'topic:', 'user prompt:', 'subject:', 'title:', 'prompt:',
        '##', '--', '==='
    ]
   
    # Check for header patterns
    if any(indicator in line_lower for indicator in header_indicators):
        return True
   
    # Check if line is very short and looks like a header
    if len(line) < 30 and (line.upper() == line or line.endswith(':')):
        return True
       
    return False

def get_latest_storyboard_stream(client: str, project: str, module: str):
    """
    Fetch the latest storyboard DOCX file from ADLS for the given client/project/module.
    Returns: (file_stream, filename)
    """
    # The directory structure should match how storyboards are saved
    storyboard_dir = f"{client}/{project}/{module}/Storyboard"
    file_system_client = get_adls_client().get_file_system_client(file_system=AZURE_STORAGE_FILESYSTEM_NAME)
    paths = list(file_system_client.get_paths(path=storyboard_dir))
    # Filter for .docx files and sort by creation time or name
    docx_files = [p for p in paths if not p.is_directory and p.name.endswith('.docx')]
    if not docx_files:
        raise FileNotFoundError("No storyboard DOCX files found in ADLS.")
    # Sort and pick the latest (by last_modified)
    latest_file = sorted(docx_files, key=lambda x: x.last_modified, reverse=True)[0]
    file_client = file_system_client.get_file_client(latest_file.name)
    file_stream = io.BytesIO(file_client.download_file().readall())
    # Get the original filename as stored in Azure (with parentheses if any)
    filename = latest_file.name.split('/')[-1]
    return file_stream, filename