import os
from rich import print as rprint
import time
import psutil
import re
from io import StringIO
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from utils.blob_operations import BlobStorageManager
import pandas as pd

class BlobStaging:
    def __init__(self):
        """Initialize the Blob extractor"""
        self.process = psutil.Process(os.getpid())
        self.vendor = 'Shell'

    def _log_operation(self, message: str, is_error: bool = False, memory: bool = True) -> None:
        """Helper method for consistent logging format"""
        mem = f"[bright_white]{self.process.memory_info().rss / (1024**2):,.0f}MB[/bright_white]" if memory else ""
        vendor_str = f" [reverse]{self.vendor}[/reverse] " if self.vendor else " " * 7
        
        message = re.sub(r'(\d+(?:\.\d+)?)', r'[#33cc99]\1[/#33cc99]', message)
        
        if is_error:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  [#FF6E6E]{message}[/#FF6E6E]")
        else:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  {message}")

    def extract_blobs(self) -> None:
        """Extract blobs from source container to destination container."""
        total_count = 0  # Initialize counter at the very start
        combined_data = pd.DataFrame()  # Initialize DataFrame at the start
        
        try:
            start_time = time.time()
            
            # Extract config values
            parent_container = "jenkins-pricing-historical"
            sub_container = self.vendor.lower()
            
            # Initialize blob managers
            blob_manager = BlobStorageManager(parent_container, sub_container)
            destination_blob_manager = BlobStorageManager(f"jenkins-pricing-staging/{self.vendor.lower()}")
            dev_blob_manager = BlobStorageManager(f"jenkins-pricing-dev")  # Add dev container manager
            
            # Get list of blobs and count them
            blobs = blob_manager.list_blobs()  # Materialize the iterator
            total_count = len(list(blob_manager.list_blobs()))
            
            self._log_operation(f"Found total of {total_count} blobs in {parent_container}", self.vendor)
            
            if total_count == 0:
                self._log_operation("No blobs found in source container", self.vendor)
                return
            
            processed_count = 0
            skipped_count = 0
            error_count = 0
            
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
                        # Check if blob already exists in dev container
                        if dev_blob_manager.blob_exists(blob.name):
                            self._log_operation(f"Skipping {blob.name} - already processed", self.vendor)
                            skipped_count += 1
                            progress.update(task, advance=1, description=f"Skipping: {blob.name[:50]}")
                            continue

                        # Read blob content and metadata
                        data = blob_manager.read_blob(blob.name)
                        
                        # Convert bytes to string and create DataFrame
                        if isinstance(data, bytes):
                            # Try different encodings
                            encodings = ['utf-8', 'utf-16', 'iso-8859-1', 'cp1252']
                            df = None
                            last_error = None
                            
                            for encoding in encodings:
                                try:
                                    data_str = data.decode(encoding)
                                    # Try different CSV parsing options
                                    try:
                                        df = pd.read_csv(StringIO(data_str))
                                        # print("Used encoding: ", encoding)
                                        break
                                    except:
                                        try:
                                            df = pd.read_csv(StringIO(data_str), sep=';')
                                            # print("Used sep: ;")
                                            break
                                        except:
                                            try:
                                                df = pd.read_csv(StringIO(data_str), sep='\t')
                                                # print("Used sep: \t")
                                                break
                                            except Exception as e:
                                                last_error = e
                                                continue
                                except UnicodeDecodeError:
                                    continue
                            
                            if df is None:
                                # If CSV parsing failed, try JSON
                                try:
                                    data_str = data.decode('utf-8')
                                    df = pd.read_json(StringIO(data_str))
                                except:
                                    raise ValueError(
                                        f"Unable to parse data from {blob.name}. "
                                        f"Last error: {str(last_error)}"
                                    )
                        else:
                            # If data is already parsed (e.g., dictionary or list)
                            df = pd.DataFrame(data)

                        # Remove duplicate rows
                        df = df.drop_duplicates()
                        
                        # Add metadata as columns
                        metadata = blob.metadata if hasattr(blob, 'metadata') else {}
                        if metadata:
                            for key, value in metadata.items():
                                df[f'metadata_{key}'] = value
                        
                        # Add source blob name as a column
                        df['source_blob'] = blob.name
                        
                        # Add to combined dataset
                        combined_data = pd.concat([combined_data, df], ignore_index=True)
                        
                        # Copy the original blob to dev container
                        dev_blob_manager.upload_blob(
                            blob_name=blob.name,
                            content_type=blob.content_settings.content_type if hasattr(blob, 'content_settings') else None,
                            data=data
                        )
                        
                        processed_count += 1
                        progress.update(task, advance=1, description=f"Processing: {blob.name[:50]}")
                        
                    except Exception as e:
                        error_count += 1
                        self._log_operation(f"Error processing blob {blob.name}: {str(e)}", self.vendor, True)
                        continue

            if processed_count > 0:
                # Save master dataset
                destination_blob_manager.upload_blob(
                    blob_name=f"{self.vendor.lower()}_historical_master.csv",
                    content_type="csv",
                    data=combined_data.to_csv(index=False)
                )

            self._log_operation(f"Staging dataset saved as {self.vendor.lower()}_historical_master.csv to jenkins-pricing-staging/{self.vendor.lower()}", self.vendor)

            # Final summary
            end_time = time.time()
            duration = end_time - start_time
            
            rprint(f"\n[#33cc99]Operation completed:[/#33cc99]")
            rprint(f"  • Total blobs found: [#33cc99]{total_count:,}[/#33cc99]")
            rprint(f"  • Successfully processed: [#33cc99]{processed_count:,}[/#33cc99]")
            rprint(f"  • Skipped: [#33cc99]{skipped_count:,}[/#33cc99]")
            if error_count > 0:
                rprint(f"  • Failed to process: [#FF6E6E]{error_count:,}[/#FF6E6E]")
            rprint(f"  • Time elapsed: [#33cc99]{duration:.2f}[/#33cc99] seconds")
            
        except Exception as e:
            self._log_operation(f"Batch processing failed: {str(e)}", self.vendor, True)
            raise

if __name__ == "__main__":
    staging = BlobStaging()
    staging.extract_blobs()