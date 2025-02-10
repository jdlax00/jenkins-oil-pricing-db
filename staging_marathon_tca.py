import re
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
from rich import print as rprint
import os
import psutil
import time
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from utils.blob_operations import BlobStorageManager

def parse_mpc_file(file):
    # Convert bytes to string if needed
    if isinstance(file, bytes):
        file = file.decode('utf-8')
        
    soup = BeautifulSoup(file, 'html.parser')
    table = soup.find('table', {'class': 'MsoNormalTable'})
    if not table:
        return pd.DataFrame()
    
    rows = table.find_all('tr')
    
    # Extract Start Date, Time and End Date, Time
    start_date = start_time = end_date = end_time = None
    for row in rows[:4]:  # First four rows contain the date/time info
        cells = row.find_all('td')
        if len(cells) >= 2:
            label = cells[0].get_text(strip=True).replace(':', '')
            value = cells[1].get_text(strip=True)
            if label == 'Start Date':
                start_date = value
            elif label == 'Start Time':
                start_time = value
            elif label == 'End Date':
                end_date = value
            elif label == 'End Time':
                end_time = value
    
    # Parse start and end datetime
    start_datetime = None
    end_datetime = None
    try:
        if start_date and start_time:
            start_datetime = datetime.strptime(f"{start_date} {start_time}", "%B %d, %Y %I:%M %p")
        if end_date and end_time:
            end_datetime = datetime.strptime(f"{end_date} {end_time}", "%B %d, %Y %I:%M %p")
    except ValueError as e:
        rprint(f"[#FF6E6E]Error parsing datetime: {e}[/#FF6E6E]")
    
    # Find the header row (looks for 'Allowance' in the first cell)
    header_row = None
    for idx, row in enumerate(rows):
        first_cell = row.find('td')
        if first_cell and first_cell.get_text(strip=True) == 'Allowance':
            header_row = row
            break
    
    if not header_row:
        return pd.DataFrame()
    
    headers = [td.get_text(strip=True) for td in header_row.find_all('td')]
    
    data = []
    for row in rows[idx+1:]:  # Process rows after header
        cells = row.find_all('td')
        if len(cells) != len(headers):
            continue  # Skip rows that don't match header length
        row_data = {}
        for header, cell in zip(headers, cells):
            cell_text = cell.get_text(strip=True)
            # Convert 'Allowance' to float
            if header == 'Allowance':
                row_data[header] = float(cell_text) if cell_text else 0.0
            else:
                row_data[header] = cell_text
        # Add datetime objects
        row_data['Start DateTime'] = start_datetime
        row_data['End DateTime'] = end_datetime
        data.append(row_data)
    
    return pd.DataFrame(data)

class MPCStaging:
    def __init__(self):
        """Initialize the Blob extractor"""
        self.process = psutil.Process(os.getpid())
        self.vendor = 'Marathon-TCA'

    def _log_operation(self, message: str, is_error: bool = False, memory: bool = True) -> None:
        """Helper method for consistent logging format"""
        mem = f"[bright_white]{self.process.memory_info().rss / (1024**2):,.0f}MB[/bright_white]" if memory else ""
        vendor_str = f" [reverse]{self.vendor}[/reverse] " if self.vendor else " " * 7
        
        message = re.sub(r'(\d+(?:\.\d+)?)', r'[#33cc99]\1[/#33cc99]', message)
        
        if is_error:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  [#FF6E6E]{message}[/#FF6E6E]")
        else:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  {message}")

    def process_mpc_files(self):
        start_time = time.time()
        blob_manager = BlobStorageManager("jenkins-pricing-historical", "marathon-tca")
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
                    df = parse_mpc_file(data)
                    if not df.empty:
                        all_data.append(df)
                    processed_count += 1
                    progress.update(task, advance=1, description=f"Processing: {blob.name[:50]}")
                except Exception as e:
                    error_count += 1
                    self._log_operation(f"Error processing blob {blob.name}: {str(e)}", is_error=True)
        
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            # Save master dataset
            destination_blob_manager.upload_blob(
                blob_name=f"{self.vendor.lower()}_historical_master.csv",
                content_type="text/csv",
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
    staging = MPCStaging()
    final_df = staging.process_mpc_files()
    if not final_df.empty:
        print(f"Successfully processed {len(final_df)} records")
    else:
        print("No data was processed")