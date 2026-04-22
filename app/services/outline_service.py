# /app/services/outline_service.py

import os
import json
import logging
from typing import List, Dict, Any, Tuple, IO

from app.models.outline_models import OutlineRequest, FullOutline, DownloadRequest
from app.models.project_models import get_client, get_project, get_module, get_file
from core.orchestrator import search_outline_pipeline
from core.save_adls import save_outline_to_adls
from core.upload_outline_on_storyboard import save_uploaded_outline_to_adls, get_outlines_from_adls
from core.upload_files_on_outline import save_source_file_to_adls
from app.models.filestructure import get_latest_outline_stream

logger = logging.getLogger(__name__)

class OutlineService:
    """Service class for outline-related business logic"""
    
    @staticmethod
    def get_client_short_name(client_name: str) -> str:
        """Generate a short name for the client"""
        if not client_name:
            return "Unknown"
        short_name = ''.join(word[0].upper() for word in client_name.split() if word)
        return short_name[:3]
    
    @staticmethod
    def get_clients() -> List[str]:
        """Get all available clients"""
        return get_client()
    
    @staticmethod
    def get_projects(client: str) -> List[str]:
        """Get all projects for a client"""
        return get_project(client)
    
    @staticmethod
    def get_modules(client: str, project: str) -> List[str]:
        """Get all modules for a client/project"""
        return get_module(client, project)
    
    @staticmethod
    def get_files(client: str, project: str, module: str = None) -> List[str]:
        """Get files for a client/project/module combination"""
        files = get_file(client, project, module)
        if not files:
            return []
        if "Source" in files.keys():
            unique_files = list(dict.fromkeys(files["Source"]))
        else:
            return []
        return unique_files
    
    @staticmethod
    def generate_outline(outline_request: OutlineRequest) -> List[Dict[str, Any]]:

        try:
            """Generate outline using the search pipeline"""
            logger.info("Processing outline generation request")
            logger.info(outline_request.dict())
            
            selection_data = {
                "client": outline_request.client,
                "project": outline_request.project,
                "module": outline_request.module,
                "files": outline_request.files,
                # "topics": outline_request.topics,
                "context_prompt": outline_request.context_prompt  # Add this line
            }
            

    
            outline = search_outline_pipeline(selection_data)

            print("Generated outline:", outline)
            
            if not outline:
                logger.warning(f"Search returned no content for request: {selection_data}")
                return []
                
            return outline
        except Exception as e:
            print("Generated outline 2:", outline)
            logger.error(f"Failed to generate outline: {e}", exc_info=True)
            return []
    
    @staticmethod
    def save_outline(full_outline: FullOutline) -> Tuple[bool, str, str]:
        """
        Save outline to ADLS
        Returns: (success: bool, message: str, path: str)
        """
        try:
            full_outline_dict = full_outline.dict()
            
            logger.info("Processing outline save request:")
            logger.info(json.dumps(full_outline_dict, indent=2))
            
            client_short_name = OutlineService.get_client_short_name(full_outline.client)
            module_name = full_outline.module.replace(" ", "")
            filename = f"{client_short_name}_{module_name}_CO.docx"
            
            logger.info(f"Calling save_outline_to_adls with filename: {filename}")
            saved_path = save_outline_to_adls(full_outline_dict, filename)
            
            if not saved_path:
                return False, "Failed to save outline to ADLS. The save operation returned no path.", ""
            
            return True, f"Outline saved to ADLS as {filename}", saved_path
            
        except Exception as e:
            logger.error(f"Failed to save outline: {e}", exc_info=True)
            return False, f"An internal server error occurred while saving the outline: {str(e)}", ""
    
    @staticmethod
    def download_outline(request: DownloadRequest) -> Tuple[IO, str]:
        """
        Get outline file stream for download
        Returns: (file_stream, filename)
        """
        logger.info(f"Processing download request: {request.dict()}")
        return get_latest_outline_stream(request.client, request.project, request.module)
    
    @staticmethod
    def get_outline_list(client: str, project: str, module: str) -> List[str]:
        """Get list of available outlines from ADLS"""
        return get_outlines_from_adls(client=client, project=project, module=module)
    
    @staticmethod
    def upload_outline_file(original_filename: str, file_bytes: bytes, 
                           client: str, project: str, module: str) -> Tuple[bool, str, str]:
        """
        Upload outline file to ADLS
        Returns: (success: bool, message: str, outline_name: str)
        """
        try:
            final_path = save_uploaded_outline_to_adls(
                original_filename=original_filename,
                file_bytes=file_bytes,
                client=client,
                project=project,
                module=module
            )
            
            if not final_path:
                return False, "Core upload function returned a null path, indicating failure.", ""
            
            return True, "Outline uploaded successfully to ADLS!", final_path
            
        except Exception as e:
            logger.error(f"Failed to upload outline file for {client}/{project}/{module}: {e}", exc_info=True)
            return False, "An internal server error occurred during file upload.", ""
    
    @staticmethod
    def upload_source_files(files_data: List[Tuple[str, bytes]], client: str, 
                           project: str, module: str = None) -> Tuple[bool, str, List[str]]:
        """
        Upload multiple source files
        Returns: (success: bool, message: str, uploaded_files: List[str])
        """
        uploaded_paths = []
        effective_module = module if module and module.strip() else ""
        
        for original_filename, file_bytes in files_data:
            try:
                saved_path = save_source_file_to_adls(
                    original_filename=original_filename,
                    file_bytes=file_bytes,
                    client=client,
                    project=project,
                    module=effective_module
                )
                
                if not saved_path:
                    return False, f"Failed to upload file: {original_filename}", []
                
                print(f"Saved path: {saved_path}")
                
                uploaded_paths.append(saved_path)
                
            except ValueError as e:
                return False, str(e), []
            except Exception as e:
                logger.error(f"Error processing file upload for {original_filename}: {e}", exc_info=True)
                return False, f"An error occurred while uploading {original_filename}: {str(e)}", []
        
        uploaded_files = [os.path.basename(p) for p in uploaded_paths]
        return True, f"{len(uploaded_paths)} file(s) uploaded successfully.", uploaded_files ,uploaded_paths
