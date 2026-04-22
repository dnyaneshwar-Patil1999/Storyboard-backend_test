import os
import mimetypes
import logging 
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from app.models.project_models import get_client, get_project, get_module
from core.adls import get_adls_client, create_sas_url, FILESYSTEM_NAME, get_file_structure
 
router = APIRouter()
 
def is_folder_structure(item_content):
    """Check if the item represents a folder structure (has subfolders/files)"""
    if not isinstance(item_content, dict):
        return False
    if any(subfolder in item_content for subfolder in ["Source", "Outline", "Storyboard", "Output"]):
        return True
    if any(isinstance(subitem, dict) for subitem in item_content.values()):
        return True
    return False
 
def get_folder_contents(client: str, project: str, module: str = "", current_path: str = "") -> dict:
    """
    Get folder structure for the view component.
    Returns folders and files at the current level.
    """
    structure = get_file_structure()
    result = {"folders": [], "files": []}
   
    try:
        if not module and not current_path:
            # Root project level
            project_data = structure.get(client, {}).get(project, {})
            for item_name, item_content in project_data.items():
                if is_folder_structure(item_content):
                    result["folders"].append(item_name)
                else:
                    result["files"].append(item_name)
       
        elif module and not current_path:
            # Module root level
            module_data = structure.get(client, {}).get(project, {}).get(module, {})
            for item_name, item_content in module_data.items():
                if is_folder_structure(item_content):
                    result["folders"].append(item_name)
                else:
                    result["files"].append(item_name)
       
        else:
            # Navigate to specific path
            path_parts = current_path.split('/')
            current_level = structure.get(client, {}).get(project, {})
           
            if module:
                current_level = current_level.get(module, {})
           
            for part in path_parts:
                if part and part in current_level:
                    current_level = current_level[part]
                else:
                    return result  # Path not found
           
            for item_name, item_content in current_level.items():
                if is_folder_structure(item_content):
                    result["folders"].append(item_name)
                else:
                    result["files"].append(item_name)
       
        # Sort results
        result["folders"] = sorted(result["folders"])
        result["files"] = sorted(result["files"])
        return result
       
    except Exception as e:
        logging.error(f"Error processing folder structure: {e}", exc_info=True)
        return result
 
@router.get("/getfoldercontents")
def get_folder_contents_endpoint(
    client: str,
    project: str,
    module: str = "",
    path: str = ""
):
    """
    New endpoint to get folder contents dynamically
    """
    try:
        contents = get_folder_contents(client, project, module, path)
        return JSONResponse(content=contents)
    except Exception as e:
        logging.error(f"Failed to get folder contents for '{client}/{project}/{module}/{path}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not retrieve folder contents.")
 
 
def get_file(client: str, project: str, module: str) -> dict[str, list[str]]:
    structure = get_file_structure()
    files_by_subfolder = {}
    subfolders_to_check = ["Source", "Outline", "Storyboard"]
    try:
        module_data = structure.get(client, {}).get(project, {}).get(module, {})
        for subfolder in subfolders_to_check:
            if subfolder in module_data and module_data[subfolder]:
                files_by_subfolder[subfolder] = sorted(list(module_data[subfolder].keys()))
    except Exception as e:
        logging.error(f"Error processing file structure for get_file: {e}", exc_info=True)
        return {} # Return empty dict safely on any error
    return files_by_subfolder
 
def get_combined_files(client: str, project: str, module: str = "") -> dict[str, list[str]]:
    """
    Get files for the view component.
    - If module is empty: returns only project-level files
    - If module is provided: returns only module-level files for that specific module
    """
    structure = get_file_structure()
    combined_files = {}
    try:
        project_data = structure.get(client, {}).get(project, {})
        # If no module specified, return only project-level files
        if not module:
            project_files = []
            for item_name, item_content in project_data.items():
                # Skip if it's a module folder (has Source/Outline/Output structure)
                if isinstance(item_content, dict) and any(
                    subfolder in item_content
                    for subfolder in ["Source", "Outline", "Output", "Storyboard"]
                ):
                    continue
                # Add if it's a file (not a directory or empty dict)
                if not isinstance(item_content, dict) or not item_content:
                    project_files.append(item_name)
            if project_files:
                combined_files["Project"] = sorted(project_files)
        # If module is provided, return only module-level files
        else:
            module_data = project_data.get(module, {})
            subfolders_to_check = ["Source", "Outline", "Storyboard"]
            for subfolder in subfolders_to_check:
                if subfolder in module_data and module_data[subfolder]:
                    combined_files[subfolder] = sorted(list(module_data[subfolder].keys()))
        return combined_files
    except Exception as e:
        logging.error(f"Error processing combined file structure: {e}", exc_info=True)
        return {}
 
# --- API Endpoints with Production-Grade Error Handling ---
 
@router.get("/getclients")
def get_client_list():
    try:
        return JSONResponse(content=get_client())
    except Exception as e:
        logging.error(f"Failed to get client list: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not retrieve client list.")
 
@router.get("/getprojects")
def get_project_list(client: str):
    try:
        return JSONResponse(content=get_project(client))
    except Exception as e:
        logging.error(f"Failed to get project list for client '{client}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not retrieve project list.")
 
@router.get("/getmodules")
def get_module_list(client: str, project: str):
    try:
        return JSONResponse(content=get_module(client, project))
    except Exception as e:
        logging.error(f"Failed to get module list for '{client}/{project}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not retrieve module list.")
 
@router.get("/getfiles")
def get_file_list(client: str, project: str, module: str):
    try:
        return JSONResponse(content=get_file(client, project, module))
    except Exception as e:
        logging.error(f"Failed to get file list for '{client}/{project}/{module}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not retrieve file list.")
 
@router.get("/getviewfiles")
def get_view_file_list(client: str, project: str, module: str = ""):
    """
    New endpoint for the view component that returns both project and module files
    """
    try:
        return JSONResponse(content=get_combined_files(client, project, module))
    except Exception as e:
        logging.error(f"Failed to get combined file list for '{client}/{project}/{module}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not retrieve combined file list.")
 
@router.get("/view-url")
def get_viewable_file_url(file_path: str = Query(..., description="Full path to file in ADLS")):
    try:
        service_client = get_adls_client()
        file_system_client = service_client.get_file_system_client(FILESYSTEM_NAME)
        source_file_client = file_system_client.get_file_client(file_path)
 
        if not source_file_client.exists():
            raise HTTPException(status_code=404, detail=f"File not found at path: {file_path}")
       
        url = create_sas_url(FILESYSTEM_NAME, file_path)
        if not url:
            raise HTTPException(status_code=500, detail="Failed to generate a secure URL for the file.")
       
        return JSONResponse(content={"status": "complete", "url": url})
 
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logging.error(f"Unexpected error generating view URL for '{file_path}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while preparing the file for viewing.")
