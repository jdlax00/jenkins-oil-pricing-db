import PyPDF2
import pandas as pd
from rich import print as rprint
import os
import psutil
import time
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from utils.blob_operations import BlobStorageManager
import re
from io import BytesIO

def parse_bradhall_file(pdf_path):
    """
    Parse BradHall PDF file using PyPDF2 and regex to extract pricing data.
    This parser is product-agnostic and will dynamically handle any product columns present in the file.
    
    Args:
        pdf_path (str): Path to the PDF file
    
    Returns:
        pd.DataFrame: Long-format DataFrame with columns:
            - location: The pricing location
            - date: The effective date
            - time: The effective time
            - product: The product type
            - price: The price value
    """
    # Lists to store our parsed data in long format
    locations = []
    dates = []
    times = []
    products = []
    prices = []
    
    try:
        pdf_reader = PyPDF2.PdfReader(pdf_path)
        current_header = None
        
        for page in pdf_reader.pages:
            text = page.extract_text()
            lines = text.split('\n')
            
            for line in lines:
                # Clean up the line
                line = re.sub(r'\s+', ' ', line).strip()
                line = line.replace('• ', '')  # Remove bullet points
                
                # Skip empty lines and known header lines
                if not line or any(header in line for header in ['JENKINS OIL', 'Tel.', 'Contact:', 'N/Q =', '*']):
                    continue
                
                # Check if this is a header line that defines products
                if 'Effective Time' in line:
                    # Extract product names from header
                    current_header = line.split('Effective Time')[1].strip().split()
                    continue
                
                # Try to match the data line pattern
                pattern = r"([A-Za-z0-9-]+(?:[ -][A-Za-z0-9-]+)*)\s+(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2})\s+((?:\d+\.\d+\s*)+)"
                match = re.match(pattern, line)
                
                if match:
                    location, date, time, values = match.groups()
                    
                    # Parse the numerical values
                    price_values = [float(v) for v in values.strip().split()]
                    
                    # If we have a header, use it to map products
                    # If not, we'll generate generic product names
                    products_for_row = (current_header[:len(price_values)] 
                                      if current_header and len(current_header) >= len(price_values)
                                      else [f'Product_{i+1}' for i in range(len(price_values))])
                    
                    # Add each product-price pair as a separate row
                    for product, price in zip(products_for_row, price_values):
                        locations.append(location.strip())
                        dates.append(date)
                        times.append(time)
                        products.append(product)
                        prices.append(price)
        
        # Create DataFrame in long format
        df = pd.DataFrame({
            'location': locations,
            'date': dates,
            'time': times,
            'product': products,
            'price': prices
        })
        
        # Convert date column to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Sort the DataFrame for better organization
        df = df.sort_values(['date', 'time', 'location', 'product']).reset_index(drop=True)
        
        return df
        
    except Exception as e:
        print(f"Error parsing PDF: {str(e)}")
        return pd.DataFrame()
    
class BradHallStaging:
    def __init__(self):
        """Initialize the Blob extractor"""
        self.process = psutil.Process(os.getpid())
        self.vendor = 'BradHall'

    def _log_operation(self, message: str, is_error: bool = False, memory: bool = True) -> None:
        """Helper method for consistent logging format"""
        mem = f"[bright_white]{self.process.memory_info().rss / (1024**2):,.0f}MB[/bright_white]" if memory else ""
        vendor_str = f" [reverse]{self.vendor}[/reverse] " if self.vendor else " " * 7
        
        message = re.sub(r'(\d+(?:\.\d+)?)', r'[#33cc99]\1[/#33cc99]', message)
        
        if is_error:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  [#FF6E6E]{message}[/#FF6E6E]")
        else:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  {message}")

    def process_bradhall_files(self):
        start_time = time.time()
        blob_manager = BlobStorageManager("jenkins-pricing-historical", "bradhall")
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
                    pdf_file = BytesIO(data)
                    df = parse_bradhall_file(pdf_file)
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
    staging = BradHallStaging()
    final_df = staging.process_bradhall_files()
    if not final_df.empty:
        print(f"Successfully processed {len(final_df)} records")
    else:
        print("No data was processed")