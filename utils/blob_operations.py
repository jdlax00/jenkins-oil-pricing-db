from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv

load_dotenv()

class BlobStorageManager:
    def __init__(self, parent_container: str, sub_container: str = None):
        connect_str = os.getenv('AZURE_WEB_JOBS_STORAGE')
        self.blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        container_path = f"{parent_container}/{sub_container}" if sub_container else parent_container
        self.container_client = self.blob_service_client.get_container_client(container_path)
    
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