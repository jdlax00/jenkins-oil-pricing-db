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

@dataclass
class PDFPage:
    page_number: int
    text_content: str
    lines: List[str]
    tables: List[List[str]]
    headers: List[str]

class PDFExtractor:
    def __init__(self, pdf_content: bytes):
        self.pdf_content = BytesIO(pdf_content)  # Changed to accept bytes and wrap in BytesIO
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
            reader = PyPDF2.PdfReader(self.pdf_content)  # Use BytesIO object directly
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
        except Exception as e:
            self.logger.error(f"Error processing PDF: {str(e)}")
            raise

# Get first pdf from blob storage
blob_manager = BlobStorageManager("jenkins-pricing-historical", "bradhall")
blobs = list(blob_manager.list_blobs())
pdf_path = blobs[10600]

# Process pdf
pdf_content = blob_manager.read_blob(pdf_path.name)
pdf_io = BytesIO(pdf_content)  # Wrap bytes in BytesIO
reader = PyPDF2.PdfReader(pdf_io)  # Pass the BytesIO object to PdfReader

def clean_text(text: str) -> str:
    """Clean and normalize the text before parsing."""
    
    # Split into lines and remove empty lines
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    
    # Remove header information
    while lines and not any(state in lines[0] for state in ['Arizona', 'Colorado', 'Nevada', 'Utah', 'Wyoming']):
        lines.pop(0)
    
    # Remove footer information
    while lines and ('N/Q = No Quote' in lines[-1] or '*' in lines[-1] or '-' * 20 in lines[-1]):
        lines.pop()
    
    # Clean up bullet points and line breaks in terminal names
    cleaned_lines = []
    current_line = ""
    
    for line in lines:
        # Skip separator lines
        if '-' * 20 in line:
            continue
            
        # If it's a state header, add with newlines
        if any(state in line for state in ['Arizona', 'Colorado', 'Nevada', 'Utah', 'Wyoming']):
            if current_line:
                cleaned_lines.append(current_line)
            cleaned_lines.append("\n" + line + "\n")
            current_line = ""
            continue
            
        # If it's a location header (contains "Effective Time")
        if 'Effective Time' in line:
            if current_line:
                cleaned_lines.append(current_line)
            # Add extra spacing around headers
            cleaned_lines.append("\n" + line + "\n")
            current_line = ""
            continue
            
        # If line contains timestamp, it's a data line
        timestamp_match = re.search(r'\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}', line)
        if timestamp_match:
            if current_line:
                # Combine with previous terminal name and normalize spacing
                full_line = re.sub(r'\s+', ' ', current_line + " " + line)
                cleaned_lines.append(full_line)
                current_line = ""
            else:
                cleaned_lines.append(line)
        else:
            # Must be terminal name or continuation
            if line.strip().startswith('•'):
                line = line.replace('•', '').strip()
            if current_line:
                current_line += " " + line.strip()
            else:
                current_line = line.strip()
    
    # Add any remaining line
    if current_line:
        cleaned_lines.append(current_line)
    
    # Join lines and fix formatting
    cleaned_text = '\n'.join(cleaned_lines)
    
    # Fix common formatting issues
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text)  # Remove extra whitespace
    cleaned_text = re.sub(r'(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2})', r'\1 \2', cleaned_text)  # Fix timestamp spacing
    cleaned_text = re.sub(r'([A-Z]{2})\s*Effective Time', r'\1\nEffective Time', cleaned_text)  # Fix header spacing
    cleaned_text = re.sub(r'-W-MtnF', '-W-MtnF ', cleaned_text)  # Fix mountain fuel spacing
    cleaned_text = re.sub(r'(\d{2,3}\.\d{4})', r'\1 ', cleaned_text)  # Add space after prices
    
    # Add newlines between sections
    cleaned_text = re.sub(r'([A-Z]{2})\n', r'\1\n\n', cleaned_text)
    
    return cleaned_text

def parse_pricing_data(text: str) -> pd.DataFrame:
    # First clean the text
    cleaned_text = clean_text(text)
    print("Cleaned text:")  # Debug
    print(cleaned_text)
    print("\n" + "="*80 + "\n")
    
    # Initialize lists to store data
    records = []
    
    # Split text into lines and remove empty lines
    lines = [line.strip() for line in cleaned_text.split('\n') if line.strip()]
    
    # Initialize variables
    current_location = ""
    current_headers = []
    
    # Regular expressions
    location_pattern = r'^([\w\s]+),\s*([A-Z]{2})\s*Effective Time'
    time_pattern = r'(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})'
    price_pattern = r'(?:^|\s)(\d{0,2}\.?\d{4}|\d{1,3}\.\d{2,4})(?:\s|$)'
    
    # Define all possible product columns in the order they might appear
    all_product_columns = [
        'C-ULSD2', 'D-ULSD2', 
        'C-ULSD2-W-MtnF', 'D-ULSD2-W-MtnF',
        'C-ULSD1', 'D-ULSD1',
        'C-ULSD2-W-ProP', 'D-ULSD2-W-ProP',
        'C-ULSD2-ProP', 'D-ULSD2-ProP',
        '85-E10', '87-E10', '88-E10', '89-E10', '91-E10', '88-MID'
    ]
    
    print("Processing lines...")  # Debug
    
    for i, line in enumerate(lines):
        # Check if line contains location information
        location_match = re.search(location_pattern, line)
        if location_match:
            current_location = f"{location_match.group(1)}, {location_match.group(2)}"
            # Look for headers in this line and the next line
            header_text = line + " " + (lines[i+1] if i+1 < len(lines) else "")
            current_headers = []
            for product in all_product_columns:
                if product in header_text:
                    current_headers.append(product)
            print(f"\nLocation: {current_location}")  # Debug
            print(f"Found headers: {current_headers}")  # Debug
            continue
            
        # Skip headers and separators
        if 'Effective Time' in line or '-' * 20 in line or 'N/Q = No Quote' in line:
            continue
            
        # Process pricing data
        if any(char.isdigit() for char in line):
            parts = line.split()
            timestamp_found = False
            terminal_parts = []
            
            for i, part in enumerate(parts):
                if re.match(time_pattern, part + ' ' + parts[i+1] if i+1 < len(parts) else ''):
                    timestamp_found = True
                    timestamp = part + ' ' + parts[i+1]
                    price_text = ' '.join(parts[i+2:])
                    prices = re.findall(price_pattern, price_text)
                    print(f"\nTerminal line: {line}")  # Debug
                    print(f"Found prices: {prices}")  # Debug
                    break
                terminal_parts.append(part)
            
            if timestamp_found:
                terminal = ' '.join(terminal_parts).strip('• ')
                
                record = {
                    'Location': current_location,
                    'Terminal': terminal,
                    'Effective_Time': timestamp,
                }
                
                # Use current headers if available, otherwise use default columns
                price_columns = current_headers if current_headers else all_product_columns[:len(prices)]
                print(f"Using columns: {price_columns}")  # Debug
                
                for i, price in enumerate(prices):
                    if i < len(price_columns):
                        try:
                            price_float = float(price)
                            # Exclude placeholder values
                            if not (8.9 <= price_float <= 10.1):
                                record[price_columns[i]] = price_float
                        except ValueError:
                            continue
                
                records.append(record)
    
    # Create DataFrame
    df = pd.DataFrame(records)
    
    # Convert timestamp to datetime
    # df['Effective_Time'] = pd.to_datetime(df['Effective_Time'], format='%m/%d/%Y %H:%M')
    
    # Sort by location and effective time
    # df = df.sort_values(['Location', 'Effective_Time'])
    
    # Ensure all possible columns exist
    for col in all_product_columns:
        if col not in df.columns:
            df[col] = None
            
    print("\nFinal columns:", df.columns.tolist())  # Debug
    
    return df


# save the text to a file from reader
all_text = ""
for page in reader.pages:
    all_text += page.extract_text() + "\n"

cleaned_text = clean_text(all_text)

df = parse_pricing_data(cleaned_text)