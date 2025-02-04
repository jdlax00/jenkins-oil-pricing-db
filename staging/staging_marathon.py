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
from bs4 import BeautifulSoup

def parse_marathon_file(file_content):
    """
    Parse Marathon price notification email content with MSO support and proper datetime handling
    """
    if isinstance(file_content, bytes):
        file_content = file_content.decode('utf-8')
        
    data = []
    current_terminal = None
    effective_datetime = None
    
    soup = BeautifulSoup(file_content, 'html.parser')
    print("\nParsing new file...")
    print(f"HTML snippet: {str(soup)[:200]}...")
    
    tables = soup.find_all('table')
    print(f"Found {len(tables)} tables")
    
    # Enhanced date parsing - Look for all elements containing "Effective:"
    for element in soup.find_all(lambda tag: tag.get_text() and 'Effective :' in tag.get_text()):
        parent_text = element.parent.get_text() if element.parent else element.get_text()
        if match := re.search(r'(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}\s+[AP]M)', parent_text):
            effective_datetime = datetime.strptime(match.group(1).strip(), '%m/%d/%Y %I:%M %p')
            print(f"Found effective datetime: {effective_datetime}")
            break
    
    if not effective_datetime:
        print("Warning: No effective datetime found in document")
        return pd.DataFrame()
    
    for idx, table in enumerate(tables):
        print(f"\nAnalyzing table {idx + 1}")
        
        # Improved terminal detection - handle both MSO and non-MSO formats
        terminal_cells = []
        # Check direct text in td cells
        terminal_cells.extend(table.find_all('td', string=lambda x: x and any(loc in str(x) for loc in ['SALT LAKE', 'LAS VEGAS'])))
        # Check for styled cells
        terminal_cells.extend(table.find_all('td', style=lambda x: x and ('font-weight:bold' in str(x) or 'font-weight: bold' in str(x))))
        # Check MSO paragraphs
        terminal_cells.extend(table.find_all('p', class_='MsoNormal'))
        
        for cell in terminal_cells:
            cell_text = cell.get_text(strip=True)
            print(f"Checking potential terminal cell: {cell_text}")
            if any(loc in cell_text for loc in ['SALT LAKE', 'LAS VEGAS']):
                current_terminal = cell_text
                print(f"Found terminal: {current_terminal}")
        
        if current_terminal:
            product_pattern = re.compile(r'^[A-Z0-9]+$')
            # Handle both MSO and non-MSO formats
            cells = table.find_all(['td', 'p'])
            
            for cell in cells:
                # Get text content, handling both formats
                cell_text = cell.get_text('\n', strip=True)
                # Clean up extra whitespace and split
                lines = [line.strip() for line in cell_text.split('\n') if line.strip()]
                
                # Skip headers and empty cells
                if not lines or lines[0] in ['Price Changes', 'New Price', 'TCA']:
                    continue
                    
                # Check if first line matches product pattern
                if not product_pattern.match(lines[0]):
                    continue
                    
                product = lines[0]
                print(f"\nFound product: {product}")
                print(f"Cell text: {cell_text}")
                print(f"Lines: {lines}")
                
                try:
                    # Handle both 3-element and 4-element formats
                    if len(lines) >= 3:
                        product = lines[0]
                        change = float(lines[1])
                        price = float(lines[2])
                        tca = float(lines[3]) if len(lines) >= 4 else 0.0
                        
                        data.append({
                            'terminal': current_terminal,
                            'product': product,
                            'price': price,
                            'change': change,
                            'tca': tca,
                            'effective_datetime': effective_datetime
                        })
                        print(f"Successfully parsed: {product} - Price: {price}, Change: {change}, TCA: {tca}")
                except (ValueError, IndexError) as e:
                    print(f"Error parsing numbers for {product}: {e}")
    
    df = pd.DataFrame(data)
    if not df.empty:
        print(f"\nSuccessfully parsed {len(df)} price records")
        print(f"Sample of parsed data:\n{df.head()}")
    else:
        print("\nNo price records were parsed")
    
    return df

class MarathonStaging:
    def __init__(self):
        """Initialize the Marathon staging processor"""
        self.process = psutil.Process(os.getpid())
        self.vendor = 'Marathon'
        
    def _log_operation(self, message: str, is_error: bool = False, memory: bool = True) -> None:
        """Helper method for consistent logging format"""
        mem = f"[bright_white]{self.process.memory_info().rss / (1024**2):,.0f}MB[/bright_white]" if memory else ""
        vendor_str = f" [reverse]{self.vendor}[/reverse] " if self.vendor else " " * 7
        
        message = re.sub(r'(\d+(?:\.\d+)?)', r'[#33cc99]\1[/#33cc99]', message)
        
        if is_error:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  [#FF6E6E]{message}[/#FF6E6E]")
        else:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  {message}")

    def process_marathon_files(self):
        start_time = time.time()
        blob_manager = BlobStorageManager("jenkins-pricing-historical", "marathon")
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
                    df = parse_marathon_file(data)
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
    staging = MarathonStaging()
    final_df = staging.process_marathon_files()
    if not final_df.empty:
        print(f"Successfully processed {len(final_df)} records")
    else:
        print("No data was processed")