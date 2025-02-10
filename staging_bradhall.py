import re
import PyPDF2
import pandas as pd
from datetime import datetime
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
import logging
from utils.blob_operations import BlobStorageManager
import os
import psutil
import time
from rich import print as rprint
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from io import BytesIO

# Define structured content containers
@dataclass
class PDFPage:
    page_number: int
    text_content: str
    lines: List[str]
    tables: List[List[str]]
    headers: List[str]

class PDFExtractor:
    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def extract_content(self) -> List[PDFPage]:
        structured_pages = []
        try:
            with open(self.pdf_path, 'rb') as file:
                reader = PyPDF2.PdfReader(file)
                for page_num in range(len(reader.pages)):
                    self.logger.info(f"Processing page {page_num + 1}")
                    page = reader.pages[page_num]
                    text = page.extract_text()
                    lines = [line.strip() for line in text.split('\n') if line.strip()]
                    tables = self.extract_tables(text)
                    headers = self.extract_headers(text)
                    structured_page = PDFPage(
                        page_number=page_num + 1,
                        text_content=text,
                        lines=lines,
                        tables=tables,
                        headers=headers
                    )
                    structured_pages.append(structured_page)
            return structured_pages
        except FileNotFoundError:
            self.logger.error(f"PDF file not found: {self.pdf_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error processing PDF: {str(e)}")
            raise

    def extract_tables(self, text: str) -> List[List[str]]:
        tables = []
        potential_rows = re.findall(r'^.*\t.*$', text, re.MULTILINE)
        if potential_rows:
            current_table = []
            for row in potential_rows:
                cells = [cell.strip() for cell in row.split('\t')]
                current_table.append(cells)
            tables.append(current_table)
        return tables

    def extract_headers(self, text: str) -> List[str]:
        headers = []
        lines = text.split('\n')
        for line in lines:
            line = line.strip()
            if (line.isupper() or
                re.match(r'^\d+\.\s+\w+', line) or
                re.match(r'^[A-Z][a-z]+(\s+[A-Z][a-z]+)*$', line)):
                headers.append(line)
        return headers

def is_date(s: str) -> bool:
    try:
        datetime.strptime(s, '%m/%d/%Y')
        return True
    except ValueError:
        return False

def is_time(s: str) -> bool:
    return re.match(r'^\d{2}:\d{2}$', s) is not None

def parse_terminal_line(line: str, current_city_info: Dict) -> Optional[Dict]:
    tokens = line.split()
    for i in range(len(tokens)):
        if is_date(tokens[i]):
            if i + 1 < len(tokens) and is_time(tokens[i+1]):
                terminal_code = ' '.join(tokens[:i])
                date_str = tokens[i]
                time_str = tokens[i+1]
                prices = [float(p) for p in tokens[i+2:] if p.replace('.', '', 1).isdigit()]
                fuel_types = current_city_info.get('fuel_types', [])
                
                # Create a dictionary for each product-price pair
                price_data = {}
                for j, ft in enumerate(fuel_types):
                    if j < len(prices):
                        price_data[ft] = prices[j]
                
                try:
                    effective_datetime = datetime.strptime(f"{date_str} {time_str}", "%m/%d/%Y %H:%M")
                except ValueError:
                    return None
                
                return {
                    'terminal_code': terminal_code,
                    'effective_datetime': effective_datetime,
                    'city': current_city_info['city'],
                    'state': current_city_info['state'],
                    'marketing_area': f"{current_city_info['city']}, {current_city_info['state']}",
                    **price_data
                }
    return None

def extract_tables(text: str) -> List[List[str]]:
    tables = []
    potential_rows = re.findall(r'^.*\t.*$', text, re.MULTILINE)
    if potential_rows:
        current_table = []
        for row in potential_rows:
            cells = [cell.strip() for cell in row.split('\t')]
            current_table.append(cells)
        tables.append(current_table)
    return tables

def extract_headers(text: str) -> List[str]:
    headers = []
    lines = text.split('\n')
    for line in lines:
        line = line.strip()
        if (line.isupper() or
            re.match(r'^\d+\.\s+\w+', line) or
            re.match(r'^[A-Z][a-z]+(\s+[A-Z][a-z]+)*$', line)):
            headers.append(line)
    return headers

def process_pdf(pdf_path: str) -> pd.DataFrame:
    # Modified to handle both file paths and BytesIO objects
    try:
        if isinstance(pdf_path, BytesIO):
            reader = PyPDF2.PdfReader(pdf_path)
        else:
            with open(pdf_path, 'rb') as file:
                reader = PyPDF2.PdfReader(file)
                
        structured_pages = []
        for page_num in range(len(reader.pages)):
            page = reader.pages[page_num]
            text = page.extract_text()
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            tables = extract_tables(text)
            headers = extract_headers(text)
            structured_page = PDFPage(
                page_number=page_num + 1,
                text_content=text,
                lines=lines,
                tables=tables,
                headers=headers
            )
            structured_pages.append(structured_page)

        # Rest of the processing remains the same
        data = []
        current_city_info = None
        current_buffer = []

        for page in structured_pages:
            for line in page.lines:
                city_match = re.match(r'^([A-Za-z\s/]+),\s*([A-Z]{2})\s+Effective Time\s+(.*)$', line)
                if city_match:
                    current_city_info = {
                        'city': city_match.group(1).strip(),
                        'state': city_match.group(2).strip(),
                        'fuel_types': city_match.group(3).split()
                    }
                    current_buffer = []
                    continue
                if not current_city_info:
                    continue
                
                combined_line = ' '.join(current_buffer + [line])
                entry = parse_terminal_line(combined_line, current_city_info)
                if entry:
                    data.append(entry)
                    current_buffer = []
                else:
                    current_buffer.append(line)

        # Create initial DataFrame
        df = pd.DataFrame(data)
        if df.empty:
            return df
            
        # Get all columns that aren't metadata
        metadata_cols = ['terminal_code', 'effective_datetime', 'city', 'state', 'marketing_area']
        product_cols = [col for col in df.columns if col not in metadata_cols]
        
        # Melt the DataFrame to pivot products into rows
        melted_df = df.melt(
            id_vars=metadata_cols,
            value_vars=product_cols,
            var_name='product',
            value_name='price'
        )
        
        # Remove rows where price is NaN
        melted_df = melted_df.dropna(subset=['price'])
        
        # Sort by datetime and location
        melted_df = melted_df.sort_values(['effective_datetime', 'marketing_area', 'terminal_code'])
        
        return melted_df
    except Exception as e:
        logging.error(f"Error processing PDF: {str(e)}")
        raise

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
                    pdf_content = blob_manager.read_blob(blob.name)
                    # Create a BytesIO object from the PDF content
                    pdf_buffer = BytesIO(pdf_content)
                    df = process_pdf(pdf_buffer)
                    if df is not None and not df.empty:
                        all_data.append(df)
                    processed_count += 1
                    progress.update(task, advance=1, description=f"Processing: {blob.name[:50]}")
                except Exception as e:
                    error_count += 1
                    self._log_operation(f"Error processing blob {blob.name}: {e}", is_error=True)
        
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            final_df = final_df.sort_values(['effective_datetime', 'marketing_area', 'terminal_code']).reset_index(drop=True)
            
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