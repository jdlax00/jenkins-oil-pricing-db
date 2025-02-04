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

def parse_bigwest_file(pdf_path):
    """
    Parse BigWest PDF file using PyPDF2 to extract pricing data.
    
    Args:
        pdf_path: Path or BytesIO object containing the PDF
    
    Returns:
        pd.DataFrame: Long-format DataFrame with columns:
            - location: The pricing location
            - date: The effective date
            - time: The effective time
            - product: The product type
            - price: The price value
    """
    locations = []
    dates = []
    times = []
    products = []
    prices = []
    
    try:
        pdf_reader = PyPDF2.PdfReader(pdf_path)
        current_header = None
        current_datetime = None
        
        def parse_header(header_line):
            """Parse the header line into correct product groupings"""
            parts = header_line.split()
            products = []
            i = 0
            while i < len(parts):
                if parts[i] == 'CVN':
                    products.append('CVN HVP')
                    i += 2
                elif parts[i] == 'UNL':
                    products.append('UNL E10 HVP')
                    i += 3
                elif parts[i] == 'MID':
                    products.append('MID E10 HVP')
                    i += 3
                elif parts[i] == 'PRE':
                    products.append('PRE E10 HVP')
                    i += 3
                elif parts[i] == 'ULSD' and i + 1 < len(parts) and parts[i + 1] == '#2':
                    products.append('ULSD #2')
                    i += 2
                elif parts[i] == 'ULSD' and i + 2 < len(parts) and parts[i + 1] == 'DYED' and parts[i + 2] == '#2':
                    products.append('ULSD DYED #2')
                    i += 3
                else:
                    i += 1
            print(f"Parsed header products: {products}")  # Debug: Show parsed products
            return products
        
        for page in pdf_reader.pages:
            text = page.extract_text()
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            
            print("\n=== New Page ===")
            for i, line in enumerate(lines):
                print(f"Line {i}: {line}")
                
                if 'Effective At:' in line:
                    datetime_str = line.replace('Effective At:', '').strip()
                    print(f"Found datetime: {datetime_str}")
                    try:
                        current_datetime = datetime_str.split()
                        print(f"Split datetime: {current_datetime}")
                    except ValueError:
                        continue
                    continue
                
                # Parse header with product names
                if i < len(lines) - 1 and any(x in line for x in ['CVN', 'UNL', 'MID', 'PRE', 'ULSD']):
                    current_header = parse_header(line)
                    print(f"Found header: {current_header}")
                    continue
                
                # Parse location and prices
                if ',' in line and any(state in line for state in ['ID', 'UT']):
                    current_location = line.strip()
                    print(f"\nProcessing location: {current_location}")
                    if i + 1 < len(lines):
                        price_line = lines[i + 1].strip()
                        print(f"Price line: {price_line}")
                        if price_line and all(c.isdigit() or c in '. ' for c in price_line):
                            price_values = [float(v) for v in price_line.split()]
                            print(f"Price values: {price_values}")
                            
                            # Map products to prices
                            products_for_row = (current_header[:len(price_values)] 
                                              if current_header and len(current_header) >= len(price_values)
                                              else [f'Product_{i+1}' for i in range(len(price_values))])
                            print(f"Products for row: {products_for_row}")
                            
                            for product, price in zip(products_for_row, price_values):
                                if current_datetime:
                                    locations.append(current_location)
                                    dates.append(current_datetime[0])
                                    times.append(current_datetime[1])
                                    products.append(product)
                                    prices.append(price)
        
        if not locations:  # If no data was parsed, return empty DataFrame
            print("No data was parsed from the PDF")
            return pd.DataFrame()
            
        # Create DataFrame
        df = pd.DataFrame({
            'location': locations,
            'date': dates,
            'time': times,
            'product': products,
            'price': prices
        })
        
        # Convert date column to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Sort the DataFrame
        df = df.sort_values(['date', 'time', 'location', 'product']).reset_index(drop=True)
        
        return df
        
    except Exception as e:
        print(f"Error parsing PDF: {str(e)}")
        return pd.DataFrame()

class BigWestStaging:
    def __init__(self):
        """Initialize the Blob extractor"""
        self.process = psutil.Process(os.getpid())
        self.vendor = 'BigWest'

    def _log_operation(self, message: str, is_error: bool = False, memory: bool = True) -> None:
        """Helper method for consistent logging format"""
        mem = f"[bright_white]{self.process.memory_info().rss / (1024**2):,.0f}MB[/bright_white]" if memory else ""
        vendor_str = f" [reverse]{self.vendor}[/reverse] " if self.vendor else " " * 7
        
        message = re.sub(r'(\d+(?:\.\d+)?)', r'[#33cc99]\1[/#33cc99]', message)
        
        if is_error:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  [#FF6E6E]{message}[/#FF6E6E]")
        else:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  {message}")

    def process_bigwest_files(self):
        start_time = time.time()
        blob_manager = BlobStorageManager("jenkins-pricing-historical", "bigwest")
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
                    df = parse_bigwest_file(pdf_file)
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
    staging = BigWestStaging()
    final_df = staging.process_bigwest_files()
    if not final_df.empty:
        print(f"Successfully processed {len(final_df)} records")
    else:
        print("No data was processed")