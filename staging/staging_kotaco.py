import pandas as pd
from bs4 import BeautifulSoup
import re
from datetime import datetime
import os
import psutil
import time
from rich import print as rprint
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from utils.blob_operations import BlobStorageManager

def parse_fuel_prices(html_content):
    # Initialize lists to store the data
    destinations = []
    suppliers = []
    products = []
    prices = []
    
    # Parse HTML content
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Extract effective date
    body_text = soup.body.get_text()
    date_match = re.search(r'(\d{2}/\d{2}/\d{2})\s*\nEffective\s*(\d{2}:\d{2})', body_text)
    if date_match:
        date_str = date_match.group(1)
        time_str = date_match.group(2)
        effective_datetime = datetime.strptime(f"{date_str} {time_str}", "%m/%d/%y %H:%M")
    else:
        effective_datetime = None
    
    # Find all table rows
    rows = soup.find_all('tr')
    
    # Initialize current destination
    current_destination = None
    
    for row in rows:
        # Look for destination headers
        destination_header = row.find('td', string=lambda text: text and 'Destination' in text)
        if destination_header and row.find_all('td')[1].find('b'):
            current_destination = row.find_all('td')[1].text.strip()
            continue
            
        # Look for data rows
        cells = row.find_all('td')
        if len(cells) >= 4:
            # Check if this is a data row (not a header row)
            if not any(['<U>' in str(cell) for cell in cells]) and not any(['<B>' in str(cell) for cell in cells]):
                destination = cells[0].text.strip()
                supplier = cells[1].text.strip()
                product = cells[2].text.strip()
                price_text = cells[3].text.strip()
                
                # Only process rows with valid data
                if all([destination, supplier, product, price_text]):
                    try:
                        price = float(price_text)
                        destinations.append(destination)
                        suppliers.append(supplier)
                        products.append(product)
                        prices.append(price)
                    except ValueError:
                        continue
    
    # Create DataFrame
    df = pd.DataFrame({
        'Terminal': destinations,
        'Supplier': suppliers,
        'Product': products,
        'Price': prices,
        'Effective_Date': effective_datetime
    })
    
    return df

class KotacoStaging:
    def __init__(self):
        """Initialize the Blob extractor"""
        self.process = psutil.Process(os.getpid())
        self.vendor = 'Kotaco'

    def _log_operation(self, message: str, is_error: bool = False, memory: bool = True) -> None:
        """Helper method for consistent logging format"""
        mem = f"[bright_white]{self.process.memory_info().rss / (1024**2):,.0f}MB[/bright_white]" if memory else ""
        vendor_str = f" [reverse]{self.vendor}[/reverse] " if self.vendor else " " * 7
        
        message = re.sub(r'(\d+(?:\.\d+)?)', r'[#33cc99]\1[/#33cc99]', message)
        
        if is_error:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  [#FF6E6E]{message}[/#FF6E6E]")
        else:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  {message}")

    def process_kotaco_files(self):
        start_time = time.time()
        blob_manager = BlobStorageManager("jenkins-pricing-historical", "kotaco")
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
                    html_content = blob_manager.read_blob(blob.name)
                    df = parse_fuel_prices(html_content)
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
    staging = KotacoStaging()
    final_df = staging.process_kotaco_files()
    if not final_df.empty:
        print(f"Successfully processed {len(final_df)} records")
    else:
        print("No data was processed")