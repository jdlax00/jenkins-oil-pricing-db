import os
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
import re
from rich import print as rprint
import psutil
import time
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from utils.blob_operations import BlobStorageManager

def extract_table_from_html(input_path):
    try:
        # Handle both file paths and byte content
        if isinstance(input_path, (str, Path)):
            # Read and parse HTML file
            with open(input_path, 'r', encoding='utf-8') as file:
                soup = BeautifulSoup(file, 'html.parser')
        else:
            # Parse bytes content directly
            soup = BeautifulSoup(input_path, 'html.parser')
        
        # Find the first table in the document
        table = soup.find('table')
        if table is None:
            raise ValueError("No table found in the HTML file")
        
        # Extract headers
        headers = []
        header_row = table.find('tr')
        if header_row:
            # Get all header cells and log their content for debugging
            header_cells = header_row.find_all(['th', 'td'])
            
            for i, cell in enumerate(header_cells):
                text = cell.get_text(strip=True)
                headers.append(text if text else f'Column_{i}')
        
        # Extract data rows with detailed logging
        data = []
        for row_idx, row in enumerate(table.find_all('tr')[1:], 1):  # Skip header row
            cells = row.find_all(['td', 'th'])
            row_data = []
            
            
            for cell in cells:
                cell_text = cell.get_text(strip=True)
                row_data.append(cell_text)
            
            # Only append non-empty rows
            if any(row_data):
                # Ensure each row has the same number of columns as headers
                if len(row_data) < len(headers):
                    # Pad with empty strings if necessary
                    row_data.extend([''] * (len(headers) - len(row_data)))
                elif len(row_data) > len(headers):
                    # Truncate extra columns
                    row_data = row_data[:len(headers)]
                
                data.append(row_data)
        
        # Create DataFrame with explicit column names
        df = pd.DataFrame(data, columns=headers)

        # Clean up the DataFrame
        df = df.dropna(how='all').dropna(axis=1, how='all')

        # Get the effective date from the data
        date_rows = df[df['Column_0'] == 'Prices Effective:']
        effective_date = None
        if not date_rows.empty:
            date_row = date_rows.iloc[0]
            # if the column is not empty and does not contain ':' then it is the effective date
            if pd.notna(date_row['Column_2']) and ':' not in date_row['Column_2']:
                effective_date = date_row['Column_2']
            elif pd.notna(date_row['Column_1']) and ':' not in date_row['Column_1']:
                effective_date = date_row['Column_1']
            else:
                effective_date = None
        
        # Initialize lists to store the extracted data
        locations = []
        products = []
        prices = []
        notes = []
        
        # Find the row index where the actual data starts (after headers)
        start_idx = df[df['Column_1'] == 'Rack City'].index[0]
        
        # Extract data row by row
        current_location = None
        
        for idx in range(start_idx, len(df)):
            row = df.iloc[idx]
            
            # Skip empty rows or rows without pricing information
            if pd.isna(row['Column_4']) or not str(row['Column_4']).replace('.', '').isdigit():
                continue
                
            # Update location if a new one is specified
            if not pd.isna(row['Column_1']):
                current_location = row['Column_1']
                
            # Extract product name, handling cases with specifications
            product = row['Column_2']
            if not pd.isna(row['Column_3']):
                product = f"{product} {row['Column_3']}"
                
            # Only append if we have valid price data
            if not pd.isna(row['Column_4']):
                locations.append(current_location)
                products.append(product)
                prices.append(float(row['Column_4']))
                notes.append(row['Column_5'] if not pd.isna(row['Column_5']) else '')
        
        # Create new dataframe with extracted information
        result_df = pd.DataFrame({
            'Location': locations,
            'Product': products,
            'Price': prices,
            'Effective Date': effective_date
        })
        
        return result_df
        
    except Exception as e:
        raise

class TartanStaging:
    def __init__(self):
        """Initialize the Blob extractor"""
        self.process = psutil.Process(os.getpid())
        self.vendor = 'Tartan'

    def _log_operation(self, message: str, is_error: bool = False, memory: bool = True) -> None:
        """Helper method for consistent logging format"""
        mem = f"[bright_white]{self.process.memory_info().rss / (1024**2):,.0f}MB[/bright_white]" if memory else ""
        vendor_str = f" [reverse]{self.vendor}[/reverse] " if self.vendor else " " * 7
        
        message = re.sub(r'(\d+(?:\.\d+)?)', r'[#33cc99]\1[/#33cc99]', message)
        
        if is_error:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  [#FF6E6E]{message}[/#FF6E6E]")
        else:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  {message}")

    def process_tartan_files(self):
        start_time = time.time()
        blob_manager = BlobStorageManager("jenkins-pricing-historical", "tartan")
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
                    df = extract_table_from_html(html_content)
                    if df is not None and not df.empty:
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
    staging = TartanStaging()
    final_df = staging.process_tartan_files()
    if not final_df.empty:
        print(f"Successfully processed {len(final_df)} records")
    else:
        print("No data was processed")