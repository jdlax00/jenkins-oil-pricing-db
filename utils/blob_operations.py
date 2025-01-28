from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv
import logging

load_dotenv()

class BlobStorageManager:
    def __init__(self, parent_container: str, sub_container: str = None):
        connect_str = os.getenv('AZURE_WEB_JOBS_STORAGE')
        self.blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        self.container_client = self.blob_service_client.get_container_client(container=f"{parent_container}")
        # self.sub_container = f"{sub_container}"
        # container_path = f"{parent_container}/{sub_container}" if sub_container else parent_container
        # self.container_client = self.blob_service_client.get_container_client(container_path)
        # self.parent_container = parent_container
        self.sub_container = sub_container
    
    def upload_blob(self, blob_name: str, data, content_type: str = None, metadata: dict = None):
        blob_client = self.container_client.get_blob_client(blob_name)
        blob_client.upload_blob(
            data,
            overwrite=True,
            content_type=content_type,
            metadata=metadata
        )
    
    def read_blob(self, blob_name: str):
        blob_client = self.container_client.get_blob_client(blob_name)
        return blob_client.download_blob().readall()
    
    def list_blobs(self):
        if self.sub_container:
            return self.container_client.list_blobs(name_starts_with=self.sub_container)
        return self.container_client.list_blobs()
    
    def blob_exists(self, blob_name: str) -> bool:
        """
        Check if a blob exists in the container
        
        Args:
            blob_name (str): Name of the blob to check
            
        Returns:
            bool: True if blob exists, False otherwise
        """
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            return blob_client.exists()
        except Exception as e:
            logging.error(f"Error checking blob existence: {str(e)}")
            return False