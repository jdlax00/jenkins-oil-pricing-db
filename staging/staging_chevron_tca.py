from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
from rich import print as rprint
import os
import psutil
import time
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from utils.blob_operations import BlobStorageManager

class ChevronPriceParser:
    def __init__(self, html_content):
        """Initialize parser with HTML content and set up BeautifulSoup.
        
        The Chevron format includes a header section with metadata and a pricing table.
        The HTML structure uses custom classes 'header-table' and 'data-table'."""
        self.soup = BeautifulSoup(html_content, 'html.parser')
        self.pricing_data = []
        self.metadata = {}
        
    def extract_metadata(self):
        """Extract metadata from the header section of the document.
        
        The metadata is stored in a table with class 'header-table', containing
        company information, notice type, and dates."""
        # Try finding the header table by class first
        metadata_table = self.soup.find('table', {'class': 'header-table'})
        
        # Fallback to looking for the first table with the expected structure
        if not metadata_table:
            metadata_table = self.soup.find('table', {'cellpadding': '0', 'cellspacing': '0', 'border': '0'})
        
        if metadata_table:
            rows = metadata_table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) == 2:
                    # Extract field name and remove colon and strong tags
                    field = cells[0].find('strong')
                    if field:
                        field = field.text.strip().replace(':', '').lower()
                        value = cells[1].text.strip()
                        self.metadata[field] = value
                    
        # Parse the effective date if present
        if 'adjustment effective date' in self.metadata:
            try:
                date_str = self.metadata['adjustment effective date']
                self.metadata['effective_datetime'] = datetime.strptime(
                    date_str, '%m-%d-%Y %H:%M'
                ).strftime('%m/%d/%y %H:%M')
            except ValueError:
                self.metadata['effective_datetime'] = None
                
        return self.metadata

    def find_price_table(self):
        """Identify the main pricing table in the document.
        
        The price table uses the class 'data-table' and contains the 
        pricing information in a structured format with headers."""
        # First try finding by class name
        price_table = self.soup.find('table', {'class': 'data-table'})
        
        # Fallback to finding by structure if class search fails
        if not price_table:
            price_table = self.soup.find('table', {
                'width': lambda x: x and x.strip() in ['600', '750'],
                'cellpadding': '0',
                'cellspacing': '0',
                'border': '1'
            })
        
        return price_table

    def extract_pricing_data(self):
        """Extract pricing information from the identified table.
        
        Processes each row of the price table, extracting values from cells
        that contain font tags with specific styling."""
        price_table = self.find_price_table()
        if not price_table:
            print("No suitable pricing table found")
            return []
            
        # Extract header row to verify column structure
        headers = []
        header_row = price_table.find('thead')
        if header_row:
            headers = [th.text.strip() for th in header_row.find_all('th')]
            
        # Process each data row in tbody
        tbody = price_table.find('tbody')
        if tbody:
            for row in tbody.find_all('tr'):
                cells = row.find_all('td')
                
                if len(cells) >= 5:  # Ensure we have all required columns
                    # Extract text from font tags or direct cell content
                    def extract_cell_text(cell):
                        font_tag = cell.find('font')
                        return (font_tag.text if font_tag else cell.text).strip()
                    
                    # Extract and clean cell values
                    terminal = extract_cell_text(cells[0])
                    product = extract_cell_text(cells[1])
                    old_price = extract_cell_text(cells[2])
                    new_price = extract_cell_text(cells[3])
                    notes = extract_cell_text(cells[4])
                    
                    # Create a record for this price entry
                    record = {
                        'Terminal': terminal,
                        'Product': product,
                        'Old Price': float(old_price.strip()),
                        'New Price': float(new_price.strip()),
                        'Notes': notes,
                        'Effective_Date': self.metadata.get('effective_datetime')
                    }
                    
                    self.pricing_data.append(record)
                
        return self.pricing_data

    def parse(self):
        """Main method to parse the document and return structured data.
        
        Extracts both metadata and pricing information, combining them into
        a complete dataset for analysis."""
        # First extract metadata to get effective date
        self.extract_metadata()
        
        # Then extract pricing data
        pricing_data = self.extract_pricing_data()
        
        return pricing_data

def convert_chevron_to_df(html_content):
    """Convert Chevron HTML price quote to DataFrame format.
    
    Args:
        html_content: Raw HTML content from Chevron price notification email
        
    Returns:
        pandas.DataFrame: Structured pricing data with columns for
        Terminal, Product, Price, Price_Change, Currency, and Effective_Date
        
    The function handles the parsing of HTML content, extraction of pricing
    data, and conversion to a pandas DataFrame for analysis."""
    try:
        # Parse the document
        parser = ChevronPriceParser(html_content)
        pricing_data = parser.parse()
        
        if not pricing_data:
            print("No pricing data was extracted")
            return None
            
        # Convert to DataFrame and sort
        df = pd.DataFrame(pricing_data)
        df = df.sort_values(['Terminal', 'Product']).reset_index(drop=True)
        
        return df
        
    except Exception as e:
        print(f"Error processing document: {str(e)}")
        return None

class ChevronStaging:
    def __init__(self):
        """Initialize the Blob extractor"""
        self.process = psutil.Process(os.getpid())
        self.vendor = 'Chevron-TCA'

    def _log_operation(self, message: str, is_error: bool = False, memory: bool = True) -> None:
        """Helper method for consistent logging format"""
        mem = f"[bright_white]{self.process.memory_info().rss / (1024**2):,.0f}MB[/bright_white]" if memory else ""
        vendor_str = f" [reverse]{self.vendor}[/reverse] " if self.vendor else " " * 7
        
        message = re.sub(r'(\d+(?:\.\d+)?)', r'[#33cc99]\1[/#33cc99]', message)
        
        if is_error:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  [#FF6E6E]{message}[/#FF6E6E]")
        else:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  {message}")

    def process_chevron_files(self):
        start_time = time.time()
        blob_manager = BlobStorageManager("jenkins-pricing-historical", "chevron-tca")
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
                    df = convert_chevron_to_df(html_content)
                    if df is not None and not df.empty:
                        all_data.append(df)
                    processed_count += 1
                    progress.update(task, advance=1, description=f"Processing: {blob.name[:50]}")
                except Exception as e:
                    error_count += 1
                    self._log_operation(f"Error processing blob {blob.name}: {e}", is_error=True)
        
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            final_df = final_df.sort_values(['Terminal', 'Product']).reset_index(drop=True)
            
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
    staging = ChevronStaging()
    final_df = staging.process_chevron_files()
    if not final_df.empty:
        print(f"Successfully processed {len(final_df)} records")
    else:
        print("No data was processed")