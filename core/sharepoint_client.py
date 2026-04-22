import os
import logging
import requests
from typing import Dict, Any, Optional
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SharePointClient:
    """SharePoint client for file upload operations using Microsoft Graph API"""
    
    def __init__(self, username: str, password: str, site_url: str):
        """Initialize SharePoint client with credentials"""
        self.username = username
        self.password = password
        self.site_url = site_url
        
        # Extract tenant and site name from URL
        if 'sharepoint.com/sites/' in site_url:
            self.tenant_name = site_url.split('sharepoint.com')[0].replace('https://', '')
            self.site_name = site_url.split('/sites/')[1].split('/')[0]
        else:
            raise ValueError("Invalid SharePoint site URL format")
        
        # API endpoints
        self.resource = f"https://{self.tenant_name}.sharepoint.com"
        
        # Authentication tokens
        self.access_token = None
        self.token_expires = None
        
        # Initialize authentication
        self._authenticate()
    
    def _authenticate(self):
        """Authenticate with SharePoint using username/password"""
        try:
            # Microsoft SharePoint Online endpoint for getting tokens
            url = "https://login.microsoftonline.com/common/oauth2/token"
            
            headers = {
                "Content-Type": "application/x-www-form-urlencoded"
            }
            
            data = {
                "grant_type": "password",
                "client_id": "2a1e110e-1c43-4b0a-8e9c-4a156622fc5c",  # Common Microsoft client ID
                "resource": self.resource,
                "username": self.username,
                "password": self.password
            }
            
            response = requests.post(url, headers=headers, data=data)
            
            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data["access_token"]
                expires_in = token_data.get("expires_in", 3600)
                self.token_expires = datetime.now().timestamp() + int(expires_in)
                logger.info("✅ SharePoint authentication successful")
            else:
                logger.error(f"❌ SharePoint authentication failed: {response.status_code} - {response.text}")
                raise Exception(f"Authentication failed: {response.status_code} - {response.text}")
        except Exception as e:
            logger.error(f"❌ SharePoint authentication failed: {e}")
            raise
    
    def _ensure_authenticated(self):
        """Ensure we have a valid token"""
        if not self.access_token or (self.token_expires and datetime.now().timestamp() > self.token_expires - 300):
            self._authenticate()
    
    def _get_site_id(self):
        """Get SharePoint site ID"""
        self._ensure_authenticated()
        
        url = f"https://graph.microsoft.com/v1.0/sites/{self.tenant_name}.sharepoint.com:/sites/{self.site_name}"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            return data["id"]
        else:
            logger.error(f"❌ Failed to get site ID: {response.status_code} - {response.text}")
            raise Exception(f"Failed to get site ID: {response.status_code} - {response.text}")
    
    def _get_drive_id(self):
        """Get SharePoint document library drive ID"""
        self._ensure_authenticated()
        site_id = self._get_site_id()
        
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            # Get the Documents drive ID (default document library)
            for drive in data["value"]:
                if drive["name"] == "Documents":
                    return drive["id"]
            
            # If no "Documents" drive found, use the first one
            if data["value"]:
                return data["value"][0]["id"]
            else:
                logger.error("❌ No drives found in the SharePoint site")
                raise Exception("No drives found in the SharePoint site")
        else:
            logger.error(f"❌ Failed to get drive ID: {response.status_code} - {response.text}")
            raise Exception(f"Failed to get drive ID: {response.status_code} - {response.text}")
    
    def _ensure_folder_exists(self, folder_path: str) -> None:
        """Create folder structure if it doesn't exist"""
        parts = folder_path.split('/')
        current_path = ""
        
        for part in parts:
            if not part:
                continue
                
            if current_path:
                current_path = f"{current_path}/{part}"
            else:
                current_path = part
                
            if not self._folder_exists(current_path):
                self._create_folder(current_path)
    
    def _folder_exists(self, folder_path: str) -> bool:
        """Check if a folder exists in SharePoint"""
        self._ensure_authenticated()
        
        site_id = self._get_site_id()
        drive_id = self._get_drive_id()
        
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{folder_path}"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        response = requests.get(url, headers=headers)
        return response.status_code == 200
    
    def _create_folder(self, folder_path: str) -> Dict[str, Any]:
        """Create a folder in SharePoint"""
        self._ensure_authenticated()
        
        site_id = self._get_site_id()
        drive_id = self._get_drive_id()
        
        # Extract parent folder path and folder name
        if '/' in folder_path:
            parent_path, folder_name = folder_path.rsplit('/', 1)
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{parent_path}:/children"
        else:
            folder_name = folder_path
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root/children"
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        data = {
            "name": folder_name,
            "folder": {},
            "@microsoft.graph.conflictBehavior": "rename"
        }
        
        response = requests.post(url, headers=headers, json=data)
        
        if response.status_code == 201:
            folder_data = response.json()
            logger.info(f"✅ Folder created successfully: {folder_path}")
            return {
                "folder_id": folder_data["id"],
                "folder_name": folder_data["name"],
                "web_url": folder_data["webUrl"]
            }
        else:
            logger.error(f"❌ Folder creation failed: {response.status_code} - {response.text}")
            raise Exception(f"Folder creation failed: {response.status_code} - {response.text}")
    
    def upload_file(self, file_content: bytes, file_name: str, folder_path: str = "") -> Dict[str, Any]:
        """Upload file to SharePoint document library"""
        self._ensure_authenticated()
        
        # Normalize the folder path format (remove trailing slashes)
        folder_path = folder_path.rstrip('/')
        
        try:
            # Create folder structure if it doesn't exist
            if folder_path:
                self._ensure_folder_exists(folder_path)
            
            # Get site and drive IDs
            site_id = self._get_site_id()
            drive_id = self._get_drive_id()
            
            # Handle filename conflicts by adding a counter if needed
            base_filename, extension = os.path.splitext(file_name)
            counter = 0
            current_filename = file_name
            
            while True:
                # Construct the upload URL
                if folder_path:
                    check_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{folder_path}/{current_filename}"
                else:
                    check_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{current_filename}"
                
                headers = {"Authorization": f"Bearer {self.access_token}"}
                response = requests.get(check_url, headers=headers)
                
                if response.status_code != 200:
                    # File doesn't exist, we can use this filename
                    break
                
                # File exists, increment counter
                counter += 1
                current_filename = f"{base_filename}({counter}){extension}"
            
            # Upload the file
            if folder_path:
                upload_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{folder_path}/{current_filename}:/content"
            else:
                upload_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{current_filename}:/content"
            
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/octet-stream"
            }
            
            response = requests.put(upload_url, headers=headers, data=file_content)
            
            if response.status_code in [200, 201]:
                file_data = response.json()
                logger.info(f"✅ File uploaded successfully: {current_filename}")
                
                # Construct a path that's compatible with the existing code's expectations
                final_path = f"{folder_path}/{current_filename}" if folder_path else current_filename
                
                return {
                    "file_id": file_data["id"],
                    "file_name": file_data["name"],
                    "download_url": file_data["webUrl"],
                    "path": final_path
                }
            else:
                logger.error(f"❌ File upload failed: {response.status_code} - {response.text}")
                raise Exception(f"File upload failed: {response.status_code} - {response.text}")
        except Exception as e:
            logger.error(f"❌ File upload failed: {e}")
            raise