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

# get first pdf from blob storage
blob_manager = BlobStorageManager("jenkins-pricing-historical", "bradhall")
destination_blob_manager = BlobStorageManager(f"jenkins-pricing-staging/bradhall")
blobs = list(blob_manager.list_blobs())
pdf_path = blobs[0]

# process pdf
pdf_content = blob_manager.read_blob(pdf_path.name)
pdf_buffer = BytesIO(pdf_content)
pdf_extractor = PDFExtractor(pdf_buffer)
pdf_extractor.extract_content()