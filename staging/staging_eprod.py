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


def parse_eprod_file(pdf_path):
    """
    Parse EProd PDF file using PyPDF2 to extract pricing data.
    
    Args:
        pdf_path: Path or BytesIO object containing the PDF
    
    Returns:
        pd.DataFrame: Long-format DataFrame with columns:
            - location: The pricing location (state + city combined)
            - product: The product type
            - base_price: The base price value
            - fee: The fee/tax amount
            - total_price: The total price value
    """
    locations = []
    products = []
    base_prices = []
    fees = []
    total_prices = []
    effective_datetime = None
    
    try:
        pdf_reader = PyPDF2.PdfReader(pdf_path)
        
        for page_num, page in enumerate(pdf_reader.pages):
            text = page.extract_text()
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            
            # Look for effective datetime on first page
            if page_num == 0:
                for line in lines:
                    if "effective on" in line.lower():
                        try:
                            datetime_str = re.search(r'effective on (\d{2}/\d{2}/\d{4} \d{2}:\d{2} [AP]M)', line, re.IGNORECASE)
                            if datetime_str:
                                effective_datetime = pd.to_datetime(datetime_str.group(1), format='%m/%d/%Y %I:%M %p')
                                break
                        except Exception:
                            pass

            for line in lines:
                # Skip footer lines or empty lines
                if any(skip in line.lower() for skip in ['if you have', 'enterprise products', 'call']):
                    continue
                    
                # Split the line into components
                parts = line.split()
                if len(parts) >= 5:  # Ensure we have enough parts for a valid price line
                    try:
                        # Last three elements should be numbers
                        total_price = float(parts[-1])
                        fee = float(parts[-2])
                        base_price = float(parts[-3])
                        
                        # Everything before the prices is location and product
                        location_product = ' '.join(parts[:-3])
                        
                        # First two characters should be state code
                        state = location_product[:2]
                        # Split remaining into city and product
                        city_product = location_product[3:].split(' ', 1)
                        if len(city_product) == 2:
                            city, product = city_product
                            # Combine state and city for location
                            location = f"{state} {city}"
                            
                            locations.append(location)
                            products.append(product)
                            base_prices.append(base_price)
                            fees.append(fee)
                            total_prices.append(total_price)
                    except (ValueError, IndexError):
                        continue
        
        if not locations:
            print("No data was parsed from the PDF")
            return pd.DataFrame(), effective_datetime
        

            
        # Create DataFrame
        df = pd.DataFrame({
            'location': locations,
            'product': products,
            'base_price': base_prices,
            'fee': fees,
            'total_price': total_prices,
            'effective_datetime': effective_datetime
        })
        
        # Sort the DataFrame
        df = df.sort_values(['location', 'product']).reset_index(drop=True)
        
        return df
        
    except Exception as e:
        print(f"Error parsing PDF: {str(e)}")
        return pd.DataFrame()

class EProdStaging:
    def __init__(self):
        """Initialize the Blob extractor"""
        self.process = psutil.Process(os.getpid())
        self.vendor = 'EProd'

    def _log_operation(self, message: str, is_error: bool = False, memory: bool = True) -> None:
        """Helper method for consistent logging format"""
        mem = f"[bright_white]{self.process.memory_info().rss / (1024**2):,.0f}MB[/bright_white]" if memory else ""
        vendor_str = f" [reverse]{self.vendor}[/reverse] " if self.vendor else " " * 7
        
        message = re.sub(r'(\d+(?:\.\d+)?)', r'[#33cc99]\1[/#33cc99]', message)
        
        if is_error:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  [#FF6E6E]{message}[/#FF6E6E]")
        else:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  {message}")

    def process_eprod_files(self):
        start_time = time.time()
        blob_manager = BlobStorageManager("jenkins-pricing-historical", "eprod")
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
                    df = parse_eprod_file(pdf_file)
                    if not df.empty:                  
                        all_data.append(df)
                    processed_count += 1
                    progress.update(task, advance=1, description=f"Processing: {blob.name[:50]}")
                except Exception as e:
                    error_count += 1
                    self._log_operation(f"Error processing blob {blob.name}: {e}", is_error=True)
        
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            # Sort by date and location
            final_df = final_df.sort_values(['location', 'product']).reset_index(drop=True)
            
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
    staging = EProdStaging()
    final_df = staging.process_eprod_files()
    if not final_df.empty:
        print(f"Successfully processed {len(final_df)} records")
    else:
        print("No data was processed")