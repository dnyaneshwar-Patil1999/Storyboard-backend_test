# # # /app/models/project_models.py

# # from core.adls import FILESYSTEM_NAME, STORAGE_ACCOUNT_KEY, STORAGE_ACCOUNT_NAME, get_file_structure
# # from typing import Optional, List, Dict
# # from azure.storage.filedatalake import DataLakeServiceClient
# # from azure.core.exceptions import ResourceNotFoundError, AzureError
# # import os
# # import logging
# # from datetime import datetime
# # from typing import Dict, Any

# # logger = logging.getLogger(__name__)

# # def get_client() -> list:
# #     """Get list of all clients"""
# #     structure = get_file_structure()
# #     print(structure)
# #     return list(structure.keys())

# # def get_project(client: str) -> list:
# #     """Get list of projects for a specific client (only folders, not files)"""
# #     structure = get_file_structure()
# #     client_data = structure.get(client, {})
# #     projects = [k for k, v in client_data.items() if isinstance(v, dict) and v]
# #     print("DEBUG: get_project returns:", projects)
# #     # Only include keys whose value is a dict (i.e., folders)
# #     return projects

# # # def get_module(client: str, project: str) -> list:
# # #     """Get list of modules for a specific client/project (only folders with module structure)"""
# # #     structure = get_file_structure()
# # #     project_data = structure.get(client, {}).get(project, {})
# # #     # Only include keys whose value is a dict and has at least one of the expected subfolders
# # #     return [
# # #         key for key, value in project_data.items()
# # #         if isinstance(value, dict) and any(
# # #             subfolder in value for subfolder in ["Source", "Outline", "Output", "Storyboard"]
# # #         )
# # #     ]

# # def get_module(client: str, project: str) -> list:
# #     """Get list of modules for a specific client/project"""
# #     structure = get_file_structure()
# #     print("DEBUG: Full structure:", structure.get(client, {}))
# #     client_data = structure.get(client, {})
# #     print("DEBUG: get_module client_data:", client_data)
# #     print("client:", client, "project:", project)
# #     project_data = client_data.get(project, {})
# #     print("DEBUG: get_module returns:", list(project_data))
# #     return [
# #         key for key, value in project_data.items()
# #         if isinstance(value, dict) and value  # Only return non-empty dictionary values
# #     ]

# # def get_file(client: str, project: str, module: Optional[str] = None) -> dict[str, list[str]]:
# #     structure = get_file_structure()
# #     try:
# #         if not module:
# #             project_data = structure.get(client, {}).get(project, {})
# #             project_files = [
# #                 item_name for item_name, item_content in project_data.items()
# #                 if not isinstance(item_content, dict) or not item_content
# #             ]
# #             return {"Project": sorted(project_files)} if project_files else {}
# #         module_data = structure.get(client, {}).get(project, {}).get(module, {})
# #         source_files = module_data.get("Source", {})
# #         return {"Source": sorted(list(source_files.keys()))} if source_files else {}
# #     except Exception as e:
# #         print(f"Error getting files: {e}")
# #         return {}

# # def get_outline_list(client: str, project: str, module: str) -> list:
# #     """Get list of outlines for a specific client/project/module"""
# #     outlines = []

# #     file_structure = get_file_structure()

# #     # Validate client
# #     if client not in file_structure:
# #         return outlines

# #     client_data = file_structure[client]

# #     # Validate project
# #     if project not in client_data:
# #         return outlines

# #     project_data = client_data[project]

# #     # Validate module
# #     if module not in project_data:
# #         return outlines

# #     module_data = project_data[module]

# #     # Get outlines from the specific module
# #     outline_files = module_data.get("Outline", {})
# #     outlines.extend(list(outline_files.keys()))

# #     return outlines



# # def get_style_guide_info(client: str) -> dict:
# #     """
# #     Get information about a client's style guide.
    
# #     Args:
# #         client: Client name
        
# #     Returns:
# #         Dict with style guide information
# #     """
# #     try:
# #         if not all([STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY, FILESYSTEM_NAME]):
# #             logger.error("Missing Azure Storage configuration.")
# #             return {"exists": False, "message": "Azure Storage not configured"}
        
# #         # Setup ADLS client
# #         account_url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
# #         service_client = DataLakeServiceClient(account_url=account_url, credential=STORAGE_ACCOUNT_KEY)
# #         file_system_client = service_client.get_file_system_client(file_system=FILESYSTEM_NAME)
        
# #         # Look for PDF files in client directory
# #         client_dir_path = f"{client}"
# #         try:
# #             file_system_client.get_directory_client(client_dir_path).get_directory_properties()
# #         except ResourceNotFoundError:
# #             return {"exists": False, "message": "Client directory not found"}
        
# #         # Find all PDF files in the client directory
# #         pdf_files = []
# #         try:
# #             paths = file_system_client.get_paths(path=client_dir_path)
# #             for path in paths:
# #                 if not path.is_directory and path.name.lower().endswith('.pdf'):
# #                     pdf_files.append(os.path.basename(path.name))
# #         except Exception as e:
# #             logger.warning(f"Error listing files in client directory: {e}")
# #             return {"exists": False, "message": f"Error accessing client directory: {e}"}
        
# #         # Return the first PDF file found, or indicate none found
# #         if pdf_files:
# #             return {
# #                 "exists": True, 
# #                 "file_name": pdf_files[0],
# #                 "message": "Style guide found"
# #             }
# #         else:
# #             return {"exists": False, "message": "No style guide found for this client"}
            
# #     except Exception as e:
# #         logger.exception(f"Error getting style guide info for client '{client}': {e}")
# #         return {"exists": False, "message": f"Error: {str(e)}"}
    
    
# # def upload_style_guide(filename: str, file_content: bytes, client: str, replace: bool = True) -> dict:
# #     """
# #     Upload style guide file to the client folder with original filename.
    
# #     Args:
# #         filename: Original filename
# #         file_content: File bytes
# #         client: Client name
# #         replace: Whether to replace existing style guide files
        
# #     Returns:
# #         Dict with status information
# #     """
# #     try:
# #         # Validate required configurations
# #         if not all([STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY, FILESYSTEM_NAME]):
# #             logger.error("Missing Azure Storage configuration.")
# #             return {"success": False, "message": "Azure Storage not properly configured"}

# #         # Setup ADLS client
# #         account_url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
# #         service_client = DataLakeServiceClient(account_url=account_url, credential=STORAGE_ACCOUNT_KEY)
# #         file_system_client = service_client.get_file_system_client(file_system=FILESYSTEM_NAME)
        
# #         # Ensure client directory exists
# #         client_dir_path = f"{client}"
# #         try:
# #             file_system_client.get_directory_client(client_dir_path).get_directory_properties()
# #         except ResourceNotFoundError:
# #             # Create client directory if it doesn't exist
# #             file_system_client.create_directory(client_dir_path)
# #             logger.info(f"Created client directory: {client_dir_path}")
        
# #         # Preserve original filename but ensure it has .pdf extension
# #         if not filename.lower().endswith('.pdf'):
# #             filename = f"{filename}.pdf"
        
# #         # Check for existing style guide files
# #         existing_style_guides = find_existing_style_guides(file_system_client, client_dir_path)
        
# #         # File path for upload
# #         file_path = f"{client}/{filename}"
        
# #         # Check if this specific file already exists
# #         file_exists = any(sg == filename for sg in existing_style_guides)
        
# #         # Handle replacement logic
# #         if file_exists or (existing_style_guides and replace):
# #             if not replace:
# #                 return {
# #                     "success": False,
# #                     "message": f"Style guide '{filename}' already exists. Set 'replace=true' to overwrite."
# #                 }
            
# #             # Delete existing files if replacing
# #             if existing_style_guides:
# #                 for old_file in existing_style_guides:
# #                     # Delete PDF
# #                     try:
# #                         old_file_path = f"{client}/{old_file}"
# #                         file_system_client.get_file_client(old_file_path).delete_file()
# #                         logger.info(f"Deleted existing style guide: {old_file_path}")
# #                     except Exception as e:
# #                         logger.warning(f"Could not delete file {old_file}: {e}")
                    
# #                     # Delete corresponding TXT file if it exists
# #                     txt_filename = os.path.splitext(old_file)[0] + ".txt"
# #                     try:
# #                         txt_file_path = f"{client}/{txt_filename}"
# #                         file_system_client.get_file_client(txt_file_path).delete_file()
# #                         logger.info(f"Deleted existing rules file: {txt_file_path}")
# #                     except Exception as e:
# #                         logger.warning(f"Could not delete rules file {txt_filename}: {e}")
        
# #         # Upload the new PDF file
# #         file_client = file_system_client.get_file_client(file_path)
# #         file_client.upload_data(file_content, overwrite=True)
# #         logger.info(f"Successfully uploaded style guide to: {file_path}")
        
# #         return {
# #             "success": True, 
# #             "message": f"Style guide {filename} uploaded successfully",
# #             "file_name": filename,
# #             "replaced": file_exists or len(existing_style_guides) > 0
# #         }
            
# #     except Exception as e:
# #         logger.exception(f"Error uploading style guide for client '{client}': {e}")
# #         return {"success": False, "message": f"Upload failed: {str(e)}"}


# # def find_existing_style_guides(file_system_client, client_dir_path) -> list[str]:
# #     """Find all existing style guide PDF files in client directory"""
# #     result = []
# #     try:
# #         paths = file_system_client.get_paths(path=client_dir_path)
# #         for path in paths:
# #             if not path.is_directory and path.name.lower().endswith('.pdf'):
# #                 # Extract just the filename from the path
# #                 filename = os.path.basename(path.name)
# #                 result.append(filename)
# #     except Exception as e:
# #         logger.warning(f"Error listing files in client directory: {e}")
    
# #     return result


# # /app/models/project_models.py

# from core.adls import FILESYSTEM_NAME, STORAGE_ACCOUNT_KEY, STORAGE_ACCOUNT_NAME, get_file_structure
# from typing import Optional, List, Dict
# from azure.storage.filedatalake import DataLakeServiceClient
# from azure.core.exceptions import ResourceNotFoundError, AzureError
# import os
# import logging
# from datetime import datetime
# from typing import Dict, Any

# logger = logging.getLogger(__name__)

# def get_client() -> list:
#     """Get list of all clients"""
#     structure = get_file_structure()
  
#     return list(structure.keys())

# def get_project(client: str) -> list:
#     """Get list of projects for a specific client (only folders, not files)"""
#     structure = get_file_structure()
#     client_data = structure.get(client, {})
#     projects = [k for k, v in client_data.items() if isinstance(v, dict) and v]
#     print("DEBUG: get_project returns:", projects)
#     # Only include keys whose value is a dict (i.e., folders)
#     return projects

# def get_module(client: str, project: str) -> list:
#     """Get list of modules for a specific client/project (only folders with module structure)"""
#     structure = get_file_structure()
#     project_data = structure.get(client, {}).get(project, {})
#     # Only include keys whose value is a dict and has at least one of the expected subfolders
#     return [
#         key for key, value in project_data.items()
#         if isinstance(value, dict) and any(
#             subfolder in value for subfolder in ["Source", "Outline", "Output", "Storyboard"]
#         )
#     ]

# def get_file(client: str, project: str, module: Optional[str] = None) -> dict[str, list[str]]:
#     structure = get_file_structure()
#     try:
#         if not module:
#             project_data = structure.get(client, {}).get(project, {})
#             project_files = [
#                 item_name for item_name, item_content in project_data.items()
#                 if not isinstance(item_content, dict) or not item_content
#             ]
#             return {"Project": sorted(project_files)} if project_files else {}
#         module_data = structure.get(client, {}).get(project, {}).get(module, {})
#         source_files = module_data.get("Source", {})
#         return {"Source": sorted(list(source_files.keys()))} if source_files else {}
#     except Exception as e:
#         print(f"Error getting files: {e}")
#         return {}

# def get_outline_list(client: str, project: str, module: str) -> list:
#     """Get list of outlines for a specific client/project/module"""
#     outlines = []

#     file_structure = get_file_structure()

#     # Validate client
#     if client not in file_structure:
#         return outlines

#     client_data = file_structure[client]

#     # Validate project
#     if project not in client_data:
#         return outlines

#     project_data = client_data[project]

#     # Validate module
#     if module not in project_data:
#         return outlines

#     module_data = project_data[module]

#     # Get outlines from the specific module
#     outline_files = module_data.get("Outline", {})
#     outlines.extend(list(outline_files.keys()))

#     return outlines



# def get_style_guide_info(client: str) -> dict:
#     """
#     Get information about a client's style guide.
#     Only searches in the client's root directory, not subdirectories.
    
#     Args:
#         client: Client name
        
#     Returns:
#         Dict with style guide information
#     """
#     try:
#         if not all([STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY, FILESYSTEM_NAME]):
#             logger.error("Missing Azure Storage configuration.")
#             return {"exists": False, "message": "Azure Storage not configured"}
        
#         # Setup ADLS client
#         account_url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
#         service_client = DataLakeServiceClient(account_url=account_url, credential=STORAGE_ACCOUNT_KEY)
#         file_system_client = service_client.get_file_system_client(file_system=FILESYSTEM_NAME)
        
#         # Look for PDF files ONLY in client root directory
#         client_dir_path = f"{client}"
#         try:
#             file_system_client.get_directory_client(client_dir_path).get_directory_properties()
#         except ResourceNotFoundError:
#             return {"exists": False, "message": "Client directory not found"}
        
#         # Find PDF files ONLY in the client root directory (not subdirectories)
#         pdf_files = []
#         try:
#             print(client)
#             # Use recursive=False to only get immediate children of client folder
#             paths = file_system_client.get_paths(path=client_dir_path, recursive=False)
#             for path in paths:
#                 print(path)
#                 # Only process files (not directories) that are directly in the client root
#                 if not path.is_directory and path.name.lower().endswith('.pdf'):
#                     # Ensure the file is directly in client root, not in subdirectories
#                     file_path_parts = path.name.split('/')
#                     if len(file_path_parts) == 2:  # client/filename.pdf (only 2 parts)
#                         pdf_files.append(os.path.basename(path.name))
                
#         except Exception as e:
#             logger.warning(f"Error listing files in client directory: {e}")
#             return {"exists": False, "message": f"Error accessing client directory: {e}"}
        
#         # Return the first PDF file found, or indicate none found
#         if pdf_files:
#             return {
#                 "exists": True, 
#                 "file_name": pdf_files[0],
#                 "message": "Style guide found"
#             }
#         else:
#             return {"exists": False, "message": "No style guide found for this client"}
            
#     except Exception as e:
#         logger.exception(f"Error getting style guide info for client '{client}': {e}")
#         return {"exists": False, "message": f"Error: {str(e)}"}
    
    
# def upload_style_guide(filename: str, file_content: bytes, client: str, replace: bool = True) -> dict:
#     """
#     Upload style guide file to the client folder with original filename.
    
#     Args:
#         filename: Original filename
#         file_content: File bytes
#         client: Client name
#         replace: Whether to replace existing style guide files
        
#     Returns:
#         Dict with status information
#     """
#     try:
#         # Validate required configurations
#         if not all([STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY, FILESYSTEM_NAME]):
#             logger.error("Missing Azure Storage configuration.")
#             return {"success": False, "message": "Azure Storage not properly configured"}

#         # Setup ADLS client
#         account_url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
#         service_client = DataLakeServiceClient(account_url=account_url, credential=STORAGE_ACCOUNT_KEY)
#         file_system_client = service_client.get_file_system_client(file_system=FILESYSTEM_NAME)
        
#         # Ensure client directory exists
#         client_dir_path = f"{client}"
#         try:
#             file_system_client.get_directory_client(client_dir_path).get_directory_properties()
#         except ResourceNotFoundError:
#             # Create client directory if it doesn't exist
#             file_system_client.create_directory(client_dir_path)
#             logger.info(f"Created client directory: {client_dir_path}")
        
#         # Preserve original filename but ensure it has .pdf extension
#         if not filename.lower().endswith('.pdf'):
#             filename = f"{filename}.pdf"
        
#         # Check for existing style guide files
#         existing_style_guides = find_existing_style_guides(file_system_client, client_dir_path)
        
#         # File path for upload
#         file_path = f"{client}/{filename}"
        
#         # Check if this specific file already exists
#         file_exists = any(sg == filename for sg in existing_style_guides)
        
#         # Handle replacement logic
#         if file_exists or (existing_style_guides and replace):
#             if not replace:
#                 return {
#                     "success": False,
#                     "message": f"Style guide '{filename}' already exists. Set 'replace=true' to overwrite."
#                 }
            
#             # Delete existing files if replacing
#             if existing_style_guides:
#                 for old_file in existing_style_guides:
#                     # Delete PDF
#                     try:
#                         old_file_path = f"{client}/{old_file}"
#                         file_system_client.get_file_client(old_file_path).delete_file()
#                         logger.info(f"Deleted existing style guide: {old_file_path}")
#                     except Exception as e:
#                         logger.warning(f"Could not delete file {old_file}: {e}")
                    
#                     # Delete corresponding TXT file if it exists
#                     txt_filename = os.path.splitext(old_file)[0] + ".txt"
#                     try:
#                         txt_file_path = f"{client}/{txt_filename}"
#                         file_system_client.get_file_client(txt_file_path).delete_file()
#                         logger.info(f"Deleted existing rules file: {txt_file_path}")
#                     except Exception as e:
#                         logger.warning(f"Could not delete rules file {txt_filename}: {e}")
        
#         # Upload the new PDF file
#         file_client = file_system_client.get_file_client(file_path)
#         file_client.upload_data(file_content, overwrite=True)
#         logger.info(f"Successfully uploaded style guide to: {file_path}")
        
#         return {
#             "success": True, 
#             "message": f"Style guide {filename} uploaded successfully",
#             "file_name": filename,
#             "replaced": file_exists or len(existing_style_guides) > 0
#         }
            
#     except Exception as e:
#         logger.exception(f"Error uploading style guide for client '{client}': {e}")
#         return {"success": False, "message": f"Upload failed: {str(e)}"}


# def find_existing_style_guides(file_system_client, client_dir_path) -> list[str]:
#     """Find all existing style guide PDF files in client directory"""
#     result = []
#     try:
#         paths = file_system_client.get_paths(path=client_dir_path)
#         for path in paths:
#             if not path.is_directory and path.name.lower().endswith('.pdf'):
#                 # Extract just the filename from the path
#                 filename = os.path.basename(path.name)
#                 result.append(filename)
#     except Exception as e:
#         logger.warning(f"Error listing files in client directory: {e}")
    
#     return result



# # /app/models/project_models.py

# from core.adls import FILESYSTEM_NAME, STORAGE_ACCOUNT_KEY, STORAGE_ACCOUNT_NAME, get_file_structure
# from typing import Optional, List, Dict
# from azure.storage.filedatalake import DataLakeServiceClient
# from azure.core.exceptions import ResourceNotFoundError, AzureError
# import os
# import logging
# from datetime import datetime
# from typing import Dict, Any

# logger = logging.getLogger(__name__)

# def get_client() -> list:
#     """Get list of all clients"""
#     structure = get_file_structure()
#     print(structure)
#     return list(structure.keys())

# def get_project(client: str) -> list:
#     """Get list of projects for a specific client (only folders, not files)"""
#     structure = get_file_structure()
#     client_data = structure.get(client, {})
#     projects = [k for k, v in client_data.items() if isinstance(v, dict) and v]
#     print("DEBUG: get_project returns:", projects)
#     # Only include keys whose value is a dict (i.e., folders)
#     return projects

# # def get_module(client: str, project: str) -> list:
# #     """Get list of modules for a specific client/project (only folders with module structure)"""
# #     structure = get_file_structure()
# #     project_data = structure.get(client, {}).get(project, {})
# #     # Only include keys whose value is a dict and has at least one of the expected subfolders
# #     return [
# #         key for key, value in project_data.items()
# #         if isinstance(value, dict) and any(
# #             subfolder in value for subfolder in ["Source", "Outline", "Output", "Storyboard"]
# #         )
# #     ]

# def get_module(client: str, project: str) -> list:
#     """Get list of modules for a specific client/project"""
#     structure = get_file_structure()
#     print("DEBUG: Full structure:", structure.get(client, {}))
#     client_data = structure.get(client, {})
#     print("DEBUG: get_module client_data:", client_data)
#     print("client:", client, "project:", project)
#     project_data = client_data.get(project, {})
#     print("DEBUG: get_module returns:", list(project_data))
#     return [
#         key for key, value in project_data.items()
#         if isinstance(value, dict) and value  # Only return non-empty dictionary values
#     ]

# def get_file(client: str, project: str, module: Optional[str] = None) -> dict[str, list[str]]:
#     structure = get_file_structure()
#     try:
#         if not module:
#             project_data = structure.get(client, {}).get(project, {})
#             project_files = [
#                 item_name for item_name, item_content in project_data.items()
#                 if not isinstance(item_content, dict) or not item_content
#             ]
#             return {"Project": sorted(project_files)} if project_files else {}
#         module_data = structure.get(client, {}).get(project, {}).get(module, {})
#         source_files = module_data.get("Source", {})
#         return {"Source": sorted(list(source_files.keys()))} if source_files else {}
#     except Exception as e:
#         print(f"Error getting files: {e}")
#         return {}

# def get_outline_list(client: str, project: str, module: str) -> list:
#     """Get list of outlines for a specific client/project/module"""
#     outlines = []

#     file_structure = get_file_structure()

#     # Validate client
#     if client not in file_structure:
#         return outlines

#     client_data = file_structure[client]

#     # Validate project
#     if project not in client_data:
#         return outlines

#     project_data = client_data[project]

#     # Validate module
#     if module not in project_data:
#         return outlines

#     module_data = project_data[module]

#     # Get outlines from the specific module
#     outline_files = module_data.get("Outline", {})
#     outlines.extend(list(outline_files.keys()))

#     return outlines



# def get_style_guide_info(client: str) -> dict:
#     """
#     Get information about a client's style guide.
    
#     Args:
#         client: Client name
        
#     Returns:
#         Dict with style guide information
#     """
#     try:
#         if not all([STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY, FILESYSTEM_NAME]):
#             logger.error("Missing Azure Storage configuration.")
#             return {"exists": False, "message": "Azure Storage not configured"}
        
#         # Setup ADLS client
#         account_url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
#         service_client = DataLakeServiceClient(account_url=account_url, credential=STORAGE_ACCOUNT_KEY)
#         file_system_client = service_client.get_file_system_client(file_system=FILESYSTEM_NAME)
        
#         # Look for PDF files in client directory
#         client_dir_path = f"{client}"
#         try:
#             file_system_client.get_directory_client(client_dir_path).get_directory_properties()
#         except ResourceNotFoundError:
#             return {"exists": False, "message": "Client directory not found"}
        
#         # Find all PDF files in the client directory
#         pdf_files = []
#         try:
#             paths = file_system_client.get_paths(path=client_dir_path)
#             for path in paths:
#                 if not path.is_directory and path.name.lower().endswith('.pdf'):
#                     pdf_files.append(os.path.basename(path.name))
#         except Exception as e:
#             logger.warning(f"Error listing files in client directory: {e}")
#             return {"exists": False, "message": f"Error accessing client directory: {e}"}
        
#         # Return the first PDF file found, or indicate none found
#         if pdf_files:
#             return {
#                 "exists": True, 
#                 "file_name": pdf_files[0],
#                 "message": "Style guide found"
#             }
#         else:
#             return {"exists": False, "message": "No style guide found for this client"}
            
#     except Exception as e:
#         logger.exception(f"Error getting style guide info for client '{client}': {e}")
#         return {"exists": False, "message": f"Error: {str(e)}"}
    
    
# def upload_style_guide(filename: str, file_content: bytes, client: str, replace: bool = True) -> dict:
#     """
#     Upload style guide file to the client folder with original filename.
    
#     Args:
#         filename: Original filename
#         file_content: File bytes
#         client: Client name
#         replace: Whether to replace existing style guide files
        
#     Returns:
#         Dict with status information
#     """
#     try:
#         # Validate required configurations
#         if not all([STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY, FILESYSTEM_NAME]):
#             logger.error("Missing Azure Storage configuration.")
#             return {"success": False, "message": "Azure Storage not properly configured"}

#         # Setup ADLS client
#         account_url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
#         service_client = DataLakeServiceClient(account_url=account_url, credential=STORAGE_ACCOUNT_KEY)
#         file_system_client = service_client.get_file_system_client(file_system=FILESYSTEM_NAME)
        
#         # Ensure client directory exists
#         client_dir_path = f"{client}"
#         try:
#             file_system_client.get_directory_client(client_dir_path).get_directory_properties()
#         except ResourceNotFoundError:
#             # Create client directory if it doesn't exist
#             file_system_client.create_directory(client_dir_path)
#             logger.info(f"Created client directory: {client_dir_path}")
        
#         # Preserve original filename but ensure it has .pdf extension
#         if not filename.lower().endswith('.pdf'):
#             filename = f"{filename}.pdf"
        
#         # Check for existing style guide files
#         existing_style_guides = find_existing_style_guides(file_system_client, client_dir_path)
        
#         # File path for upload
#         file_path = f"{client}/{filename}"
        
#         # Check if this specific file already exists
#         file_exists = any(sg == filename for sg in existing_style_guides)
        
#         # Handle replacement logic
#         if file_exists or (existing_style_guides and replace):
#             if not replace:
#                 return {
#                     "success": False,
#                     "message": f"Style guide '{filename}' already exists. Set 'replace=true' to overwrite."
#                 }
            
#             # Delete existing files if replacing
#             if existing_style_guides:
#                 for old_file in existing_style_guides:
#                     # Delete PDF
#                     try:
#                         old_file_path = f"{client}/{old_file}"
#                         file_system_client.get_file_client(old_file_path).delete_file()
#                         logger.info(f"Deleted existing style guide: {old_file_path}")
#                     except Exception as e:
#                         logger.warning(f"Could not delete file {old_file}: {e}")
                    
#                     # Delete corresponding TXT file if it exists
#                     txt_filename = os.path.splitext(old_file)[0] + ".txt"
#                     try:
#                         txt_file_path = f"{client}/{txt_filename}"
#                         file_system_client.get_file_client(txt_file_path).delete_file()
#                         logger.info(f"Deleted existing rules file: {txt_file_path}")
#                     except Exception as e:
#                         logger.warning(f"Could not delete rules file {txt_filename}: {e}")
        
#         # Upload the new PDF file
#         file_client = file_system_client.get_file_client(file_path)
#         file_client.upload_data(file_content, overwrite=True)
#         logger.info(f"Successfully uploaded style guide to: {file_path}")
        
#         return {
#             "success": True, 
#             "message": f"Style guide {filename} uploaded successfully",
#             "file_name": filename,
#             "replaced": file_exists or len(existing_style_guides) > 0
#         }
            
#     except Exception as e:
#         logger.exception(f"Error uploading style guide for client '{client}': {e}")
#         return {"success": False, "message": f"Upload failed: {str(e)}"}


# def find_existing_style_guides(file_system_client, client_dir_path) -> list[str]:
#     """Find all existing style guide PDF files in client directory"""
#     result = []
#     try:
#         paths = file_system_client.get_paths(path=client_dir_path)
#         for path in paths:
#             if not path.is_directory and path.name.lower().endswith('.pdf'):
#                 # Extract just the filename from the path
#                 filename = os.path.basename(path.name)
#                 result.append(filename)
#     except Exception as e:
#         logger.warning(f"Error listing files in client directory: {e}")
    
#     return result


# /app/models/project_models.py

from core.adls import FILESYSTEM_NAME, STORAGE_ACCOUNT_KEY, STORAGE_ACCOUNT_NAME, get_file_structure ,CHUNKS_CONTAINER_NAME
from typing import Optional, List, Dict
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceNotFoundError, AzureError
import os
import logging
from datetime import datetime
from typing import Dict, Any
import re

logger = logging.getLogger(__name__)


ORIGINAL_CONTAINER = FILESYSTEM_NAME
PROCESSED_CONTAINER = CHUNKS_CONTAINER_NAME
FILESYSTEM_NAME = ORIGINAL_CONTAINER 

def get_client() -> list:
    """Get list of all clients"""
    structure = get_file_structure()
    return list(structure.keys())

def get_project(client: str) -> list:
    """Get list of projects for a specific client (only folders, not files)"""
    structure = get_file_structure()
    client_data = structure.get(client, {})
    projects = [k for k, v in client_data.items() if isinstance(v, dict) and v]
    print("DEBUG: get_project returns:", projects)
    # Only include keys whose value is a dict (i.e., folders)
    return projects

def get_module(client: str, project: str) -> list:
    """Get list of modules for a specific client/project (only folders with module structure)"""
    structure = get_file_structure()
    project_data = structure.get(client, {}).get(project, {})
    # Only include keys whose value is a dict and has at least one of the expected subfolders
    return [
        key for key, value in project_data.items()
        if isinstance(value, dict) and any(
            subfolder in value for subfolder in ["Source", "Outline", "Output", "Storyboard"]
        )
    ]

from typing import Optional
from azure.storage.filedatalake import DataLakeServiceClient


def get_adls_client():
    account_url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=STORAGE_ACCOUNT_KEY)



# def normalize_filename(filename: str, client: str, project: str, module: Optional[str]) -> str:
#     """
#     Removes client/project/module prefixes and chunk suffixes
#     from processed filenames for accurate comparison with original names.
#     """
#     # Remove chunk suffix
#     filename = filename.split("-chunk-")[0]

#     # Remove prefix like "Client-Project-Module X-" or "Client-Project-"
#     prefix_parts = [client, project]
#     if module:
#         prefix_parts.append(module)
#     prefix_pattern = "-".join(prefix_parts) + "-"
#     if filename.startswith(prefix_pattern):
#         filename = filename[len(prefix_pattern):]

#     return filename.strip()


# def get_file(client: str, project: str, module: Optional[str] = None) -> dict[str, list[str]]:
#     """
#     Fetches files from both the original and processed ADLS containers.
#     - If module is None → fetch project-level only.
#     - If module is provided → fetch that module's 'Source' folder only.
#     Returns only those original files which have at least one processed chunk.
#     """
#     try:
#         service_client = get_adls_client()
#         print(ORIGINAL_CONTAINER,PROCESSED_CONTAINER)
#         print("**************")
#         original_container = service_client.get_file_system_client(ORIGINAL_CONTAINER)
#         processed_container = service_client.get_file_system_client(PROCESSED_CONTAINER)

#         # ✅ Determine ADLS folder path
#         if module:
#             original_path = f"{client}/{project}/{module}/Source"
#             processed_path = f"{client}/{project}/{module}"
#         else:
#             original_path = f"{client}/{project}"
#             processed_path = f"{client}/{project}"

#         # ✅ Fetch file lists from ADLS
#         original_files = [
#             f.name.split("/")[-1]
#             for f in original_container.get_paths(original_path)
#             if not f.is_directory
#         ]

#         processed_files = [
#             f.name.split("/")[-1]
#             for f in processed_container.get_paths(processed_path)
#             if f.name.endswith(".json") and not f.is_directory
#         ]

#         print(f"📁 {'Module' if module else 'Project'}-level files for {client}/{project}{'/' + module if module else ''}:")
#         print(f"Original Source Files ({len(original_files)}): {original_files}")
#         print(f"Processed Chunk Files ({len(processed_files)}): {processed_files[:5]} ...")

#         # ✅ Normalize processed filenames for comparison
#         processed_originals = [
#             normalize_filename(fname, client, project, module)
#             for fname in processed_files
#         ]

#         print(f"Extracted Originals: {processed_originals[:5]} ...")

#         # ✅ Keep only those original files that have a processed chunk
#         valid_files = [
#             orig for orig in original_files
#             if orig in processed_originals
#         ]

#         print(f"✅ Valid (both exist): {valid_files}")

#         return {"Source": sorted(valid_files)} if valid_files else {}

#     except Exception as e:
#         print(f"❌ Error getting files: {e}")
#         return {}


def normalize_filename(filename: str, client: str, project: str, module: Optional[str]) -> str:
    """
    Removes client/project/module prefixes and chunk suffixes
    from processed filenames for accurate comparison with original names.
    """
    # Remove chunk suffix
    filename = filename.split("-chunk-")[0]

    # Remove prefix like "Client-Project-Module X-" or "Client-Project-"
    prefix_parts = [client, project]
    if module:
        prefix_parts.append(module)
    prefix_pattern = "-".join(prefix_parts) + "-"
    if filename.startswith(prefix_pattern):
        filename = filename[len(prefix_pattern):]

    return filename.strip()


def get_file(client: str, project: str, module: Optional[str] = None) -> dict[str, list[str]]:
    """
    Fetches files from both the original and processed ADLS containers.
    - If module is None → fetch project-level only.
    - If module is provided → fetch that module's 'Source' folder only.
    Returns only those original files which have at least one processed chunk.
    """
    try:
        service_client = get_adls_client()
        print(ORIGINAL_CONTAINER,PROCESSED_CONTAINER)
        print("**************")
        original_container = service_client.get_file_system_client(ORIGINAL_CONTAINER)
        processed_container = service_client.get_file_system_client(PROCESSED_CONTAINER)

        # ✅ Determine ADLS folder path
        if module:
            original_path = f"{client}/{project}/{module}/Source"
            # MODIFIED: Processed chunks are also inside the 'Source' folder when a module is present
            processed_path = f"{client}/{project}/{module}/Source" 
        else:
            original_path = f"{client}/{project}"
            processed_path = f"{client}/{project}"

        # ✅ Fetch file lists from ADLS
        original_files = [
            f.name.split("/")[-1]
            for f in original_container.get_paths(original_path)
            if not f.is_directory
        ]

        processed_files = [
            f.name.split("/")[-1]
            for f in processed_container.get_paths(processed_path)
            if f.name.endswith(".json") and not f.is_directory
        ]

        print(f"📁 {'Module' if module else 'Project'}-level files for {client}/{project}{'/' + module if module else ''}:")
        print(f"Original Source Files ({len(original_files)}): {original_files}")
        print(f"Processed Chunk Files ({len(processed_files)}): {processed_files[:5]} ...")

        # ✅ Normalize processed filenames for comparison
        processed_originals = [
            normalize_filename(fname, client, project, module)
            for fname in processed_files
        ]

        print(f"Extracted Originals: {processed_originals[:5]} ...")

        # ✅ Keep only those original files that have a processed chunk
        valid_files = [
            orig for orig in original_files
            if orig in processed_originals
        ]

        print(f"✅ Valid (both exist): {valid_files}")

        return {"Source": sorted(valid_files)} if valid_files else {}

    except Exception as e:
        print(f"❌ Error getting files: {e}")
        return {}
    

def get_outline_list(client: str, project: str, module: str) -> list:
    """Get list of outlines for a specific client/project/module"""
    outlines = []

    file_structure = get_file_structure()

    # Validate client
    if client not in file_structure:
        return outlines

    client_data = file_structure[client]

    # Validate project
    if project not in client_data:
        return outlines

    project_data = client_data[project]

    # Validate module
    if module not in project_data:
        return outlines

    module_data = project_data[module]

    # Get outlines from the specific module
    outline_files = module_data.get("Outline", {})
    outlines.extend(list(outline_files.keys()))

    return outlines



def get_style_guide_info(client: str) -> dict:
    """
    Get information about a client's style guide.
    Only searches in the client's root directory, not subdirectories.
    
    Args:
        client: Client name
        
    Returns:
        Dict with style guide information
    """
    try:
        if not all([STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY, FILESYSTEM_NAME]):
            logger.error("Missing Azure Storage configuration.")
            return {"exists": False, "message": "Azure Storage not configured"}
        
        # Setup ADLS client
        account_url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
        service_client = DataLakeServiceClient(account_url=account_url, credential=STORAGE_ACCOUNT_KEY)
        file_system_client = service_client.get_file_system_client(file_system=FILESYSTEM_NAME)
        
        # Look for PDF files ONLY in client root directory
        client_dir_path = f"{client}"
        try:
            file_system_client.get_directory_client(client_dir_path).get_directory_properties()
        except ResourceNotFoundError:
            return {"exists": False, "message": "Client directory not found"}
        
        # Find PDF files ONLY in the client root directory (not subdirectories)
        pdf_files = []
        try:
            print(client)
            # Use recursive=False to only get immediate children of client folder
            paths = file_system_client.get_paths(path=client_dir_path, recursive=False)
            for path in paths:
                print(path)
                # Only process files (not directories) that are directly in the client root
                if not path.is_directory and path.name.lower().endswith('.pdf'):
                    # Ensure the file is directly in client root, not in subdirectories
                    file_path_parts = path.name.split('/')
                    if len(file_path_parts) == 2:  # client/filename.pdf (only 2 parts)
                        pdf_files.append(os.path.basename(path.name))
                
        except Exception as e:
            logger.warning(f"Error listing files in client directory: {e}")
            return {"exists": False, "message": f"Error accessing client directory: {e}"}
        
        # Return the first PDF file found, or indicate none found
        if pdf_files:
            return {
                "exists": True,
                "file_name": pdf_files[0],
                "message": "Style guide found"
            }
        else:
            return {"exists": False, "message": "No style guide found for this client"}
            
    except Exception as e:
        logger.exception(f"Error getting style guide info for client '{client}': {e}")
        return {"exists": False, "message": f"Error: {str(e)}"}
    
def upload_style_guide(filename: str, file_content: bytes, client: str, replace: bool = True) -> dict:
    """
    Upload style guide file to the client folder with original filename.
    
    Args:
        filename: Original filename
        file_content: File bytes
        client: Client name
        replace: Whether to replace existing style guide files
        
    Returns:
        Dict with status information
    """
    try:
        # Validate required configurations
        if not all([STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY, FILESYSTEM_NAME]):
            logger.error("Missing Azure Storage configuration.")
            return {"success": False, "message": "Azure Storage not properly configured"}

        # Setup ADLS client
        account_url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
        service_client = DataLakeServiceClient(account_url=account_url, credential=STORAGE_ACCOUNT_KEY)
        file_system_client = service_client.get_file_system_client(file_system=FILESYSTEM_NAME)
        
        # Ensure client directory exists
        client_dir_path = f"{client}"
        try:
            file_system_client.get_directory_client(client_dir_path).get_directory_properties()
        except ResourceNotFoundError:
            # Create client directory if it doesn't exist
            file_system_client.create_directory(client_dir_path)
            logger.info(f"Created client directory: {client_dir_path}")
        
        # Preserve original filename but ensure it has .pdf extension
        if not filename.lower().endswith('.pdf'):
            filename = f"{filename}.pdf"
        
        # Check for existing style guide files
        existing_style_guides = find_existing_style_guides(file_system_client, client_dir_path)
        
        # File path for upload
        file_path = f"{client}/{filename}"
        
        # Check if this specific file already exists
        file_exists = any(sg == filename for sg in existing_style_guides)
        
        # Handle replacement logic
        if file_exists or (existing_style_guides and replace):
            if not replace:
                return {
                    "success": False,
                    "message": f"Style guide '{filename}' already exists. Set 'replace=true' to overwrite."
                }
            
            # Delete existing files if replacing
            if existing_style_guides:
                for old_file in existing_style_guides:
                    # Delete PDF
                    try:
                        old_file_path = f"{client}/{old_file}"
                        file_system_client.get_file_client(old_file_path).delete_file()
                        logger.info(f"Deleted existing style guide: {old_file_path}")
                    except Exception as e:
                        logger.warning(f"Could not delete file {old_file}: {e}")
                    
                    # Delete corresponding TXT file if it exists
                    txt_filename = os.path.splitext(old_file)[0] + ".txt"
                    try:
                        txt_file_path = f"{client}/{txt_filename}"
                        file_system_client.get_file_client(txt_file_path).delete_file()
                        logger.info(f"Deleted existing rules file: {txt_file_path}")
                    except Exception as e:
                        logger.warning(f"Could not delete rules file {txt_filename}: {e}")
        
        # Upload the new PDF file
        file_client = file_system_client.get_file_client(file_path)
        file_client.upload_data(file_content, overwrite=True)
        logger.info(f"Successfully uploaded style guide to: {file_path}")
        
        return {
            "success": True, 
            "message": f"Style guide {filename} uploaded successfully",
            "file_name": filename,
            "replaced": file_exists or len(existing_style_guides) > 0
        }
            
    except Exception as e:
        logger.exception(f"Error uploading style guide for client '{client}': {e}")
        return {"success": False, "message": f"Upload failed: {str(e)}"}


def find_existing_style_guides(file_system_client, client_dir_path) -> list[str]:
    """Find all existing style guide PDF files in client directory"""
    result = []
    try:
        paths = file_system_client.get_paths(path=client_dir_path)
        for path in paths:
            if not path.is_directory and path.name.lower().endswith('.pdf'):
                # Extract just the filename from the path
                filename = os.path.basename(path.name)
                result.append(filename)
    except Exception as e:
        logger.warning(f"Error listing files in client directory: {e}")
    
    return result
