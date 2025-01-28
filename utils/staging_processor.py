import pandas as pd
from rich import print as rprint
import os
import psutil
import time
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from utils.blob_operations import BlobStorageManager
import re
from typing import Optional, Dict, Any

class VendorStaging:
    """Base class for vendor-specific staging operations."""
    
    def __init__(self, vendor_name: str):
        """Initialize the staging processor.
        
        Args:
            vendor_name (str): Name of the vendor being processed
        """
        self.process = psutil.Process(os.getpid())
        self.vendor = vendor_name

    def _log_operation(self, message: str, is_error: bool = False, memory: bool = True) -> None:
        """Helper method for consistent logging format.
        
        Args:
            message (str): Message to log
            is_error (bool): Whether this is an error message
            memory (bool): Whether to include memory usage information
        """
        mem = f"[bright_white]{self.process.memory_info().rss / (1024**2):,.0f}MB[/bright_white]" if memory else ""
        vendor_str = f" [reverse]{self.vendor}[/reverse] " if self.vendor else " " * 7
        
        message = re.sub(r'(\d+(?:\.\d+)?)', r'[#33cc99]\1[/#33cc99]', message)
        
        if is_error:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  [#FF6E6E]{message}[/#FF6E6E]")
        else:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  {message}")

    def parse_vendor_file(self, file_content: bytes) -> pd.DataFrame:
        """Parse vendor-specific file content. Must be implemented by subclasses.
        
        Args:
            file_content (bytes): Raw file content from blob storage
            
        Returns:
            pd.DataFrame: Processed data in standardized format
        """
        raise NotImplementedError("Subclasses must implement parse_vendor_file")

    def process_vendor_files(self) -> pd.DataFrame:
        """Process all files for a specific vendor.
        
        Returns:
            pd.DataFrame: Combined processed data from all files
        """
        start_time = time.time()
        
        # Initialize blob managers
        blob_manager = BlobStorageManager("jenkins-pricing-historical", self.vendor.lower())
        destination_blob_manager = BlobStorageManager(f"jenkins-pricing-staging/{self.vendor.lower()}")
        
        # List and count blobs
        blobs = list(blob_manager.list_blobs())
        total_count = len(blobs)
        
        self._log_operation(f"Found total of {total_count} blobs in jenkins-pricing-historical")
        
        if total_count == 0:
            self._log_operation("No blobs found in source container")
            return pd.DataFrame()
        
        processed_count = 0
        error_count = 0
        all_data = []

        # Process blobs with progress tracking
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("({task.completed}/{task.total})"),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task("Processing blobs...", total=total_count)
            
            for blob in blobs:
                try:
                    # Read and parse blob
                    data = blob_manager.read_blob(blob.name)
                    df = self.parse_vendor_file(data)
                    
                    if not df.empty:
                        all_data.append(df)
                    processed_count += 1
                    progress.update(task, advance=1, description=f"Processing: {blob.name[:50]}")
                except Exception as e:
                    error_count += 1
                    self._log_operation(f"Error processing blob {blob.name}: {e}", is_error=True)
        
        # Combine and save results
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            
            # Save master dataset
            destination_blob_manager.upload_blob(
                blob_name=f"{self.vendor.lower()}_historical_master.csv",
                content_type="csv",
                data=final_df.to_csv(index=False)
            )

            self._log_operation(
                f"Staging dataset saved as {self.vendor.lower()}_historical_master.csv "
                f"to jenkins-pricing-staging/{self.vendor.lower()}"
            )

            # Final summary
            end_time = time.time()
            duration = end_time - start_time
            
            rprint(f"\n[#33cc99]Operation completed:[/#33cc99]")
            rprint(f"  • Total blobs found: [#33cc99]{total_count:,}[/#33cc99]")
            rprint(f"  • Successfully processed: [#33cc99]{processed_count:,}[/#33cc99]")
            if error_count > 0:
                rprint(f"  • Failed to process: [#FF6E6E]{error_count:,}[/#FF6E6E]")
            rprint(f"  • Time elapsed: [#33cc99]{duration:.2f}[/#33cc99] seconds")
            
            return final_df
        return pd.DataFrame()