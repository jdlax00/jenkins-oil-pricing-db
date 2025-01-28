import re
from datetime import datetime
from utils.blob_operations import BlobStorageManager
import pandas as pd
from io import StringIO
from rich import print as rprint
import os
import psutil
import time
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn

def parse_sinclair_file(file):
    # Convert bytes to string if needed
    if isinstance(file, bytes):
        file = file.decode('utf-8')
        
    data = []
    lines = file.splitlines()
    
    # Skip header lines until we find the location header
    start_processing = False
    line_num = 0
    for line in lines:
        line = line.strip()
        line_num += 1

        # get brand from third line it will be in the format of "Sincliar Branded" or "Sincliar Unbranded"
        if line_num == 3:
            brand = line.split()[1]
            supplier = line.split()[0]
        
        if 'LOCATION' in line and 'EFF DATE' in line:
            start_processing = True
            continue
            
        if start_processing and line and not line.startswith('--') and not line.startswith('SIN1'):
            # Parse data lines
            # Expected format: Location, Date, Time, Product, Change, Price
            pattern = r'(.*?)\s{2,}(\d{2}/\d{2}/\d{2})\s+(\d{2}:\d{2})\s+(.*?)\s{2,}([+-]?\d+\.\d+)\s+(\d+\.\d+)'
            match = re.match(pattern, line)
            
            if match:
                location, date, time, product, change, price = match.groups()
                
                # Convert date and time strings to datetime
                datetime_str = f"{date} {time}"
                effective_datetime = datetime.strptime(datetime_str, '%m/%d/%y %H:%M')
                
                data.append({
                    'location': location.strip(),
                    'product': product.strip(),
                    'effective_datetime': effective_datetime,
                    'price': float(price),
                    'change': float(change),
                    'brand': brand,
                    'supplier': supplier
                })
    
    return pd.DataFrame(data)

class SinclairStaging:
    def __init__(self):
        """Initialize the Blob extractor"""
        self.process = psutil.Process(os.getpid())
        self.vendor = 'Sinclair'

    def _log_operation(self, message: str, is_error: bool = False, memory: bool = True) -> None:
        """Helper method for consistent logging format"""
        mem = f"[bright_white]{self.process.memory_info().rss / (1024**2):,.0f}MB[/bright_white]" if memory else ""
        vendor_str = f" [reverse]{self.vendor}[/reverse] " if self.vendor else " " * 7
        
        message = re.sub(r'(\d+(?:\.\d+)?)', r'[#33cc99]\1[/#33cc99]', message)
        
        if is_error:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  [#FF6E6E]{message}[/#FF6E6E]")
        else:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  {message}")

    def process_sinclair_files(self):
        start_time = time.time()
        blob_manager = BlobStorageManager("jenkins-pricing-historical", "sinclair")
        destination_blob_manager = BlobStorageManager(f"jenkins-pricing-staging/{self.vendor.lower()}")
        blobs = list(blob_manager.list_blobs())
        total_count = len(blobs)
        
        self._log_operation(f"Found total of {total_count} blobs in jenkins-pricing-historical")
        
        if total_count == 0:
            self._log_operation("No blobs found in source container")
            return pd.DataFrame()
        
        processed_count = 0
        error_count = 0
        all_data = []

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
                    data = blob_manager.read_blob(blob.name)
                    df = parse_sinclair_file(data)
                    all_data.append(df)
                    processed_count += 1
                    progress.update(task, advance=1, description=f"Processing: {blob.name[:50]}")
                except Exception as e:
                    error_count += 1
                    self._log_operation(f"Error processing blob {blob.name}: {e}", is_error=True)
        
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            # Save master dataset
            destination_blob_manager.upload_blob(
                blob_name=f"{self.vendor.lower()}_historical_master.csv",
                content_type="csv",
                data=final_df.to_csv(index=False)
            )

            self._log_operation(f"Staging dataset saved as {self.vendor.lower()}_historical_master.csv to jenkins-pricing-staging/{self.vendor.lower()}")

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

if __name__ == "__main__":
    staging = SinclairStaging()
    final_df = staging.process_sinclair_files()
    if not final_df.empty:
        print(f"Successfully processed {len(final_df)} records")
    else:
        print("No data was processed") 