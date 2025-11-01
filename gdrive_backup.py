#!/usr/bin/env python3
"""
Google Drive Backup Manager
Handles automated backup of simulation results to Google Drive
"""

import os
import io
import pickle
import tarfile
import logging
from pathlib import Path
from typing import Optional, List
from datetime import datetime

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

# Google Drive API scopes
SCOPES = ['https://www.googleapis.com/auth/drive.file']


class GoogleDriveBackup:
    """Manage backups to Google Drive."""
    
    def __init__(
        self,
        credentials_file: str = 'credentials.json',
        token_file: str = 'token.json',
        folder_id: Optional[str] = None
    ):
        """
        Initialize Google Drive backup manager.
        
        Args:
            credentials_file: Path to OAuth credentials JSON
            token_file: Path to store/load auth token
            folder_id: Google Drive folder ID (None = root)
        """
        self.credentials_file = credentials_file
        self.token_file = token_file
        self.folder_id = folder_id
        self.service = None
        self._authenticate()
    
    def _authenticate(self):
        """Authenticate with Google Drive API."""
        creds = None
        
        # Load existing token
        if os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'rb') as token:
                    creds = pickle.load(token)
                logger.info("Loaded existing Google Drive credentials")
            except Exception as e:
                logger.warning(f"Failed to load token: {e}")
        
        # Refresh or get new credentials
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                    logger.info("Refreshed Google Drive credentials")
                except Exception as e:
                    logger.error(f"Failed to refresh credentials: {e}")
                    creds = None
            
            if not creds:
                if not os.path.exists(self.credentials_file):
                    logger.error(f"Credentials file not found: {self.credentials_file}")
                    raise FileNotFoundError(
                        f"Please download OAuth credentials from Google Cloud Console "
                        f"and save as {self.credentials_file}"
                    )
                
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_file, SCOPES
                )
                creds = flow.run_local_server(port=0)
                logger.info("Obtained new Google Drive credentials")
            
            # Save credentials
            with open(self.token_file, 'wb') as token:
                pickle.dump(creds, token)
        
        # Build service
        try:
            self.service = build('drive', 'v3', credentials=creds)
            logger.info("Google Drive API service initialized")
        except Exception as e:
            logger.error(f"Failed to build Drive service: {e}")
            raise
    
    def create_folder(self, folder_name: str, parent_id: Optional[str] = None) -> str:
        """
        Create a folder in Google Drive.
        
        Args:
            folder_name: Name of folder to create
            parent_id: Parent folder ID (None = root)
            
        Returns:
            Created folder ID
        """
        try:
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            if parent_id:
                file_metadata['parents'] = [parent_id]
            
            folder = self.service.files().create(
                body=file_metadata,
                fields='id'
            ).execute()
            
            folder_id = folder.get('id')
            logger.info(f"Created folder '{folder_name}' (ID: {folder_id})")
            return folder_id
        
        except HttpError as error:
            logger.error(f"Failed to create folder: {error}")
            raise
    
    def upload_file(
        self,
        file_path: Path,
        folder_id: Optional[str] = None,
        filename: Optional[str] = None
    ) -> str:
        """
        Upload a file to Google Drive.
        
        Args:
            file_path: Path to file to upload
            folder_id: Destination folder ID
            filename: Custom filename (default: use original)
            
        Returns:
            Uploaded file ID
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        try:
            file_metadata = {
                'name': filename or file_path.name
            }
            
            if folder_id:
                file_metadata['parents'] = [folder_id]
            elif self.folder_id:
                file_metadata['parents'] = [self.folder_id]
            
            media = MediaFileUpload(
                str(file_path),
                resumable=True
            )
            
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id,name,size'
            ).execute()
            
            file_id = file.get('id')
            file_name = file.get('name')
            file_size = int(file.get('size', 0))
            
            logger.info(
                f"Uploaded '{file_name}' ({file_size / (1024*1024):.2f} MB) "
                f"(ID: {file_id})"
            )
            
            return file_id
        
        except HttpError as error:
            logger.error(f"Failed to upload file: {error}")
            raise
    
    def upload_directory(
        self,
        directory: Path,
        compress: bool = True,
        folder_id: Optional[str] = None
    ) -> str:
        """
        Upload a directory to Google Drive.
        
        Args:
            directory: Path to directory
            compress: Compress as tar.gz before upload
            folder_id: Destination folder ID
            
        Returns:
            Uploaded file/folder ID
        """
        if not directory.exists():
            raise FileNotFoundError(f"Directory not found: {directory}")
        
        if compress:
            # Create compressed archive
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_name = f"{directory.name}_{timestamp}.tar.gz"
            archive_path = directory.parent / archive_name
            
            logger.info(f"Compressing directory: {directory}")
            
            with tarfile.open(archive_path, "w:gz") as tar:
                tar.add(directory, arcname=directory.name)
            
            try:
                file_id = self.upload_file(archive_path, folder_id)
                return file_id
            finally:
                # Clean up archive
                if archive_path.exists():
                    archive_path.unlink()
        else:
            # Create folder and upload contents
            folder_name = directory.name
            remote_folder_id = self.create_folder(folder_name, folder_id)
            
            for item in directory.rglob('*'):
                if item.is_file():
                    relative_path = item.relative_to(directory)
                    self.upload_file(item, remote_folder_id, str(relative_path))
            
            return remote_folder_id
    
    def list_files(self, folder_id: Optional[str] = None, query: Optional[str] = None) -> List[dict]:
        """
        List files in Google Drive folder.
        
        Args:
            folder_id: Folder ID to list (None = root)
            query: Additional query filter
            
        Returns:
            List of file metadata dictionaries
        """
        try:
            # Build query
            query_parts = []
            
            if folder_id:
                query_parts.append(f"'{folder_id}' in parents")
            elif self.folder_id:
                query_parts.append(f"'{self.folder_id}' in parents")
            
            if query:
                query_parts.append(query)
            
            query_str = ' and '.join(query_parts) if query_parts else None
            
            results = self.service.files().list(
                q=query_str,
                spaces='drive',
                fields='files(id, name, mimeType, size, createdTime, modifiedTime)',
                pageSize=100
            ).execute()
            
            files = results.get('files', [])
            logger.info(f"Found {len(files)} files")
            return files
        
        except HttpError as error:
            logger.error(f"Failed to list files: {error}")
            return []
    
    def delete_file(self, file_id: str):
        """
        Delete a file from Google Drive.
        
        Args:
            file_id: File ID to delete
        """
        try:
            self.service.files().delete(fileId=file_id).execute()
            logger.info(f"Deleted file (ID: {file_id})")
        except HttpError as error:
            logger.error(f"Failed to delete file: {error}")
            raise
    
    def backup_results(
        self,
        results_dir: Path,
        compress: bool = True,
        delete_after: bool = False
    ) -> Optional[str]:
        """
        Backup simulation results directory to Google Drive.
        
        Args:
            results_dir: Path to results directory
            compress: Compress before upload
            delete_after: Delete local copy after successful upload
            
        Returns:
            File ID of uploaded backup, or None if failed
        """
        try:
            logger.info(f"Backing up results: {results_dir}")
            
            file_id = self.upload_directory(
                results_dir,
                compress=compress,
                folder_id=self.folder_id
            )
            
            if delete_after and file_id:
                logger.info(f"Deleting local copy: {results_dir}")
                import shutil
                shutil.rmtree(results_dir)
            
            return file_id
        
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            return None


if __name__ == "__main__":
    # Test Google Drive backup
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python gdrive_backup.py <directory_to_backup>")
        sys.exit(1)
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    directory = Path(sys.argv[1])
    
    if not directory.exists():
        print(f"Directory not found: {directory}")
        sys.exit(1)
    
    try:
        backup = GoogleDriveBackup()
        file_id = backup.upload_directory(directory, compress=True)
        print(f"\n✓ Successfully uploaded to Google Drive")
        print(f"  File ID: {file_id}")
    except Exception as e:
        print(f"\n✗ Backup failed: {e}")
        sys.exit(1)
