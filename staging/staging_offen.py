import pandas as pd
import re
from datetime import datetime, timedelta
import psutil
import os
import time
from io import BytesIO
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from rich import print as rprint
from utils.blob_operations import BlobStorageManager

def is_location_row(row):
    """
    Determine if a row contains a location header.
    A location row typically has:
    1. First cell in all caps
    2. Contains product codes in subsequent cells
    3. No pricing numbers in the first cell
    """
    if not row[0] or not isinstance(row[0], str):
        return False
        
    first_cell = str(row[0]).strip()
    # Check if first cell is in uppercase and contains no numbers
    if not first_cell.isupper() or any(c.isdigit() for c in first_cell):
        return False
        
    # Check if subsequent cells contain typical product codes
    product_cells = [str(cell).strip() for cell in row[1:] if pd.notna(cell)]
    has_product_codes = any(cell.startswith(('LF', 'PL', 'D#', 'PDF')) for cell in product_cells)
    
    return has_product_codes

def get_price_value(price_str):
    """
    Convert price string to appropriate format.
    Returns:
    - float value for valid prices
    - None for truly missing values
    - 9.0 for N/A values (marked as 9)
    """
    if pd.isna(price_str) or str(price_str).strip() == '':
        return None
    try:
        price = float(price_str)
        return price  # Return all prices, including 9.0
    except (ValueError, TypeError):
        if str(price_str).strip().startswith('*'):
            return None
        return None

def parse_xls_file(file_path):
    """
    Parse XLS file for oil pricing data with specific formatting requirements.
    
    Args:
        file_path: Path to the XLS file
    
    Returns:
        pd.DataFrame: DataFrame with columns:
            - Location: Full city/location name
            - Terminal: Terminal/supplier name with original formatting
            - Product: Standardized product code
            - Price: Numerical price value (includes 9.0 for N/A)
            - Effective: Full date range string
            - Notes: Additional notes (e.g., midnight-midnight pricing)
    """
    try:
        # Try reading with openpyxl first (for xlsx files)
        try:
            df = pd.read_excel(file_path, engine='openpyxl', header=None)
        except:
            # If that fails, try xlrd (for xls files)
            df = pd.read_excel(file_path, engine='xlrd', header=None)
        
        # Remove completely empty rows
        df_raw = df.dropna(how='all')
        
        # Initialize lists to store our parsed data
        records = []
        
        # Extract effective date range from the first few rows
        effective_date = None
        current_location = None
        products = None
        
        # Convert DataFrame to list for easier processing
        rows = df_raw.values.tolist()
        
        # First pass: find the effective date range
        for row in rows[:10]:
            row_str = ' '.join(str(x) for x in row if pd.notna(x))
            if isinstance(row[0], str) and re.search(r'\d{1,2}/\d{1,2}/\d{4}.*-.*\d{1,2}/\d{1,2}/\d{4}', row[0]):
                effective_date = row[0].strip()
                break
        
        # Second pass: process the data
        for i, row in enumerate(rows):
            # Convert row values to strings and clean them
            row = [x if pd.notna(x) else '' for x in row]  # Keep original values for price conversion
            
            # Skip empty rows or header rows
            if not any(row) or any(header.lower() in ' '.join(str(x) for x in row).lower() 
                for header in ['unnamed:', 'confidential', 'customer:', 'from:']):
                continue
            
            # Check if this is a location row
            if is_location_row(row):
                current_location = row[0].strip()
                # The products are in the same row for this format
                products = [x for x in row[1:] if x and not str(x).startswith('*')]
                continue
            
            # Process price rows
            if current_location and products and row[0] and not str(row[0]).startswith('*'):
                terminal = row[0]
                notes = row[-1] if len(row) > len(products) and row[-1] else None
                
                # Add each product and its price
                for j, product in enumerate(products):
                    price_idx = j + 1
                    if price_idx < len(row):
                        price = get_price_value(row[price_idx])
                        records.append({
                            'Location': current_location,
                            'Terminal': terminal,
                            'Product': product,
                            'Price': price,
                            'Effective': effective_date,
                            'Notes': notes
                        })
                    else:
                        # Product exists but no price column for it
                        records.append({
                            'Location': current_location,
                            'Terminal': terminal,
                            'Product': product,
                            'Price': None,
                            'Effective': effective_date,
                            'Notes': notes
                        })
        
        # Create final DataFrame
        final_df = pd.DataFrame(records)
        
        if final_df.empty:
            rprint(f"[#FF6E6E]Warning: No data was extracted from {file_path}[/#FF6E6E]")
            return pd.DataFrame()
            
        # Sort the DataFrame
        final_df = final_df.sort_values(['Location', 'Terminal', 'Product']).reset_index(drop=True)
        
        return final_df
    
    except Exception as e:
        print(f"Error parsing file: {str(e)}")
        return pd.DataFrame()

class OffenStaging:
    def __init__(self):
        """Initialize the Blob extractor"""
        self.process = psutil.Process(os.getpid())
        self.vendor = 'Offen'

    def _log_operation(self, message: str, is_error: bool = False, memory: bool = True) -> None:
        """Helper method for consistent logging format"""
        mem = f"[bright_white]{self.process.memory_info().rss / (1024**2):,.0f}MB[/bright_white]" if memory else ""
        vendor_str = f" [reverse]{self.vendor}[/reverse] " if self.vendor else " " * 7
        
        message = re.sub(r'(\d+(?:\.\d+)?)', r'[#33cc99]\1[/#33cc99]', message)
        
        if is_error:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  [#FF6E6E]{message}[/#FF6E6E]")
        else:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  {message}")

    def process_offen_files(self):
        start_time = time.time()
        blob_manager = BlobStorageManager("jenkins-pricing-historical", "offen")
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
                    # Create a BytesIO object from the blob data
                    excel_file = BytesIO(data)
                    df = parse_xls_file(excel_file)
                    if not df.empty:                  
                        all_data.append(df)
                    processed_count += 1
                    progress.update(task, advance=1, description=f"Processing: {blob.name[:50]}")
                except Exception as e:
                    error_count += 1
                    self._log_operation(f"Error processing blob {blob.name}: {e}", is_error=True)
        
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            # Sort by location and terminal
            final_df = final_df.sort_values(['Location', 'Terminal', 'Product']).reset_index(drop=True)
            
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
    staging = OffenStaging()
    final_df = staging.process_offen_files()
    if not final_df.empty:
        print(f"Successfully processed {len(final_df)} records")
    else:
        print("No data was processed")