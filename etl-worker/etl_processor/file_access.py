# etl-worker/etl_processor/file_access.py
import os
import requests
import pandas as pd
from io import BytesIO
import logging
from typing import Optional, Dict, List, Any

logger = logging.getLogger(__name__)

class WindowsFileAccess:
    """Client for accessing files from the Windows file server"""
    
    def __init__(self, fileserver_url: Optional[str] = None):
        self.base_url = fileserver_url or os.getenv('FILESERVER_URL', 'http://fileserver:5000')
        self.session = requests.Session()
        self.timeout = 30
    
    def health_check(self) -> Dict[str, Any]:
        """Check if the file server is healthy"""
        try:
            response = self.session.get(
                f"{self.base_url}/health",
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Health check failed: {str(e)}")
            raise
    
    def list_files(self) -> Dict[str, Any]:
        """List available Excel files"""
        try:
            response = self.session.get(
                f"{self.base_url}/list",
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Failed to list files: {str(e)}")
            raise
    
    def get_sheet_names(self, filename: str) -> List[str]:
        """Get sheet names from an Excel file"""
        try:
            response = self.session.get(
                f"{self.base_url}/sheets/{filename}",
                timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()
            return data.get('sheets', [])
        except requests.RequestException as e:
            logger.error(f"Failed to get sheets for {filename}: {str(e)}")
            raise
    
    def read_excel(self, filename: str, sheet_name: Optional[Any] = 0, nrows: Optional[int] = None) -> pd.DataFrame:
        """Read Excel file from Windows container"""
        try:
            # For complex Excel files, download and parse locally
            if nrows is None:
                response = self.session.get(
                    f"{self.base_url}/download/{filename}",
                    timeout=self.timeout
                )
                response.raise_for_status()
                return pd.read_excel(BytesIO(response.content), sheet_name=sheet_name)
            
            # For simpler cases, use JSON endpoint
            response = self.session.post(
                f"{self.base_url}/read_with_params",
                json={
                    "filename": filename,
                    "sheet_name": sheet_name,
                    "nrows": nrows
                },
                timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()
            return pd.DataFrame(data['data'])
            
        except requests.RequestException as e:
            logger.error(f"Failed to read {filename}: {str(e)}")
            raise
    
    def download_file(self, filename: str, save_path: Optional[str] = None) -> bytes:
        """Download a file from the Windows container"""
        try:
            response = self.session.get(
                f"{self.base_url}/download/{filename}",
                timeout=self.timeout
            )
            response.raise_for_status()
            
            if save_path:
                with open(save_path, 'wb') as f:
                    f.write(response.content)
                logger.info(f"Saved {filename} to {save_path}")
            
            return response.content
            
        except requests.RequestException as e:
            logger.error(f"Failed to download {filename}: {str(e)}")
            raise