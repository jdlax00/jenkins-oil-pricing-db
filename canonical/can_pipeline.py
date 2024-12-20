import azure.functions as func
from utils.blob_operations import BlobStorageManager
import pandas as pd

def consolidate_data() -> None:
    clean_blob_manager = BlobStorageManager("cleaned-data-container")
    master_blob_manager = BlobStorageManager("master-data-container")
    
    # Get all cleaned data
    cleaned_files = clean_blob_manager.list_blobs()
    
    # Combine data
    combined_data = pd.DataFrame()
    for file in cleaned_files:
        data = clean_blob_manager.read_blob(file.name)
        # Add to combined dataset
        combined_data = pd.concat([combined_data, pd.DataFrame(data)])
    
    # Save master dataset
    master_blob_manager.upload_blob("master_dataset.parquet", combined_data) 