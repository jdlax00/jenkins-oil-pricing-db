from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime
from rich import print as rprint
import os
import psutil
import time
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from utils.blob_operations import BlobStorageManager

class PriceQuoteParser:
    def __init__(self, html_content):
        """Initialize parser with HTML content and set up BeautifulSoup."""
        self.soup = BeautifulSoup(html_content, 'html.parser')
        self.pricing_data = []
        
    def find_date_pattern(self):
        """Search for date patterns in the document using multiple approaches."""
        # Common date-related keywords
        date_keywords = ['date', 'effective', 'as of', 'valid']
        
        # Look for date patterns in table cells
        for cell in self.soup.find_all(['td', 'th']):
            text = cell.text.strip().lower()
            
            # Check if cell contains date-related keyword
            if any(keyword in text for keyword in date_keywords):
                # Look for date in this cell and next siblings
                cells = [cell] + cell.find_next_siblings()
                for c in cells:
                    # Look for date patterns
                    text = c.text.strip()
                    # Try different date formats
                    date_formats = [
                        ('%m/%d/%Y', r'\d{1,2}/\d{1,2}/\d{4}'),
                        ('%m/%d/%y', r'\d{1,2}/\d{1,2}/\d{2}'),
                        ('%Y-%m-%d', r'\d{4}-\d{1,2}-\d{1,2}')
                    ]
                    
                    for date_format, pattern in date_formats:
                        match = re.search(pattern, text)
                        if match:
                            try:
                                date_str = match.group(0)
                                # Look for time pattern
                                time_match = re.search(r'(\d{1,2}:\d{2})\s*(am|pm|AM|PM)?', text)
                                if time_match:
                                    time_str = time_match.group(0)
                                    datetime_str = f"{date_str} {time_str}"
                                    try:
                                        if 'm' in time_str.lower():  # AM/PM format
                                            dt = datetime.strptime(datetime_str, f"{date_format} %I:%M%p")
                                        else:  # 24-hour format
                                            dt = datetime.strptime(datetime_str, f"{date_format} %H:%M")
                                        return dt.strftime("%m/%d/%y %H:%M")
                                    except ValueError:
                                        continue
                                else:
                                    dt = datetime.strptime(date_str, date_format)
                                    return dt.strftime("%m/%d/%y")
                            except ValueError:
                                continue
        return None

    def find_price_table(self):
        """Identify the main pricing table by looking for common patterns."""
        potential_tables = []
        
        # Look through all tables in the document
        tables = self.soup.find_all('table')
        for table in tables:
            price_indicators = 0
            rows = table.find_all('tr')
            
            # Skip tables with too few rows
            if len(rows) < 3:
                continue
                
            # Look for price-related content
            for row in rows:
                cells = row.find_all(['td', 'th'])
                row_text = ' '.join(cell.text.strip().lower() for cell in cells)
                
                # Check for price-related patterns
                if re.search(r'\d+\.\d+', row_text):  # Decimal numbers
                    price_indicators += 1
                if any(word in row_text for word in ['price', 'product', 'terminal', 'available']):
                    price_indicators += 1
                    
            if price_indicators > 3:  # Arbitrary threshold
                potential_tables.append((table, price_indicators))
                
        # Sort by number of indicators and return the best match
        if potential_tables:
            return sorted(potential_tables, key=lambda x: x[1], reverse=True)[0][0]
        return None

    def extract_pricing_data(self):
        """Extract pricing information from the identified table."""
        price_table = self.find_price_table()
        if not price_table:
            print("No suitable pricing table found")
            return []
            
        rows = price_table.find_all('tr')
        current_terminal = None
        current_group = None
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                continue
                
            row_text = ' '.join(cell.text.strip() for cell in cells).lower()
            
            # Try to identify terminal information
            if any(term in row_text for term in ['terminal', 'location', 'facility']):
                continue  # Skip header rows
                
            # Process the cells
            cell_texts = [cell.text.strip() for cell in cells]
            
            # Look for terminal names (usually longer text without numbers)
            if any(len(text) > 15 and not re.search(r'\d', text) for text in cell_texts):
                potential_terminal = next((text for text in cell_texts if len(text) > 15 and not re.search(r'\d', text)), None)
                if potential_terminal:
                    current_terminal = potential_terminal
                    continue
            
            # Look for product and price information
            product = None
            price = None
            
            for text in cell_texts:
                # Look for price (decimal number)
                if re.match(r'^\d+\.\d+$', text):
                    price = text
                # Look for product name (non-numeric, reasonable length)
                elif len(text) > 2 and not re.match(r'^\d', text):
                    if not product:  # Take the first potential product name
                        product = text
                    
            # Handle product groups
            if product and not price and not any(char.isdigit() for char in product):
                current_group = product
                continue
                
            # If we found both product and price
            if product and price and current_terminal:
                final_product = f"{current_group} {product}" if current_group else product
                self.pricing_data.append({
                    'Terminal': current_terminal,
                    'Product': final_product.strip(),
                    'Price': price,
                    'Effective_Date': None  # Will be filled in later
                })
                
        return self.pricing_data

    def parse(self):
        """Main method to parse the document and return structured data."""
        effective_date = self.find_date_pattern()
        pricing_data = self.extract_pricing_data()
        
        # Add effective date to all entries
        for entry in pricing_data:
            entry['Effective_Date'] = effective_date
            
        return pricing_data

def convert_html_to_df(html_content):
    """Convert HTML price quote to CSV format."""
    try:
        # Parse the document
        parser = PriceQuoteParser(html_content)
        pricing_data = parser.parse()
        
        if not pricing_data:
            print("No pricing data was extracted")
            return False
            
        df = pd.DataFrame(pricing_data)
        return df
        
    except Exception as e:
        print(f"Error processing document: {str(e)}")
        return False

class ProPetroStaging:
    def __init__(self):
        """Initialize the Blob extractor"""
        self.process = psutil.Process(os.getpid())
        self.vendor = 'ProPetro'

    def _log_operation(self, message: str, is_error: bool = False, memory: bool = True) -> None:
        """Helper method for consistent logging format"""
        mem = f"[bright_white]{self.process.memory_info().rss / (1024**2):,.0f}MB[/bright_white]" if memory else ""
        vendor_str = f" [reverse]{self.vendor}[/reverse] " if self.vendor else " " * 7
        
        message = re.sub(r'(\d+(?:\.\d+)?)', r'[#33cc99]\1[/#33cc99]', message)
        
        if is_error:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  [#FF6E6E]{message}[/#FF6E6E]")
        else:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  {message}")

    def process_propetro_files(self):
        start_time = time.time()
        blob_manager = BlobStorageManager("jenkins-pricing-historical", "propetro")
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
                    parser = PriceQuoteParser(html_content)
                    df = pd.DataFrame(parser.parse())
                    if not df.empty:
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
    staging = ProPetroStaging()
    final_df = staging.process_propetro_files()
    if not final_df.empty:
        print(f"Successfully processed {len(final_df)} records")
    else:
        print("No data was processed")