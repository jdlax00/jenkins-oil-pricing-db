import re
from utils.blob_operations import BlobStorageManager
import pandas as pd
from rich import print as rprint
import os
import psutil
import time
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn

def parse_bbenergy_file(file):
    # Convert bytes to string if needed
    if isinstance(file, bytes):
        file = file.decode('utf-8')
    
    locations = []
    dates = []
    times = []
    products = []
    changes = []
    prices = []
    
    lines = file.splitlines()
    in_data_section = False
    
    for line in lines:
        line = line.strip()
        
        if not line:
            continue
            
        if "LOCATION" in line and "EFF DATE" in line:
            in_data_section = True
            continue
            
        if line.startswith('----'):
            continue
            
        if line.startswith('BB ') or line.startswith('BBE1'):
            continue
            
        if in_data_section:
            # Updated pattern to match the actual format
            pattern = r"([A-Za-z, -]+?)\s+(\d{2}/\d{2}/\d{2})\s+(\d{2}:\d{2})\s+([A-Za-z0-9 -]+?)\s+([-+]?\d+\.\d{4}|[-+]?\d+\.\d{3}|[-+]?\d+\.\d{2}|[-+]?\d+\.\d{1})\s+(\d+\.\d{4}|\d+\.\d{3}|\d+\.\d{2}|\d+\.\d{1})"
            
            # Clean up multiple spaces in the line
            line = re.sub(r'\s+', ' ', line).strip()
            match = re.match(pattern, line)
            
            if match:
                location, date, time, product, change, price = match.groups()
                locations.append(location.strip())
                dates.append(date)
                times.append(time)
                products.append(product.strip())
                try:
                    changes.append(float(change))
                    prices.append(float(price))
                except ValueError:
                    continue
    
    # Create DataFrame instead of list of dictionaries
    df = pd.DataFrame({
        'location': locations,
        'date': dates,
        'time': times,
        'product': products,
        'change': changes,
        'price': prices
    })
    
    return df

class BBEnergyStaging:
    def __init__(self):
        """Initialize the Blob extractor"""
        self.process = psutil.Process(os.getpid())
        self.vendor = 'BBEnergy'

    def _log_operation(self, message: str, is_error: bool = False, memory: bool = True) -> None:
        """Helper method for consistent logging format"""
        mem = f"[bright_white]{self.process.memory_info().rss / (1024**2):,.0f}MB[/bright_white]" if memory else ""
        vendor_str = f" [reverse]{self.vendor}[/reverse] " if self.vendor else " " * 7
        
        message = re.sub(r'(\d+(?:\.\d+)?)', r'[#33cc99]\1[/#33cc99]', message)
        
        if is_error:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  [#FF6E6E]{message}[/#FF6E6E]")
        else:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  {message}")

    def process_bbenergy_files(self):
        start_time = time.time()
        blob_manager = BlobStorageManager("jenkins-pricing-historical", "bbenergy")
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
                    df = parse_bbenergy_file(data)  # Now returns a DataFrame directly
                    if not df.empty:
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
    staging = BBEnergyStaging()
    final_df = staging.process_bbenergy_files()
    if not final_df.empty:
        print(f"Successfully processed {len(final_df)} records")
    else:
        print("No data was processed") 