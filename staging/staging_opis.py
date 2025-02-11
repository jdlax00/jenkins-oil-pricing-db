import pandas as pd
import re
from rich import print as rprint
import os
import psutil
import time
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from utils.blob_operations import BlobStorageManager

def convert_opis_to_df(content):
    """Convert OPIS text content to DataFrame format.
    
    Args:
        content: Raw text content from OPIS price file
        
    Returns:
        pandas.DataFrame: Structured pricing data
    """
    try:
        # Split content into lines
        lines = content.splitlines()
        
        all_data = []
        current_section = None
        marketing_area = None
        section_data = []
        
        for i, line in enumerate(lines):
            line = line.rstrip()
            
            # Check for marketing area
            if i < len(lines) - 1 and "**OPIS NET TERMINAL" in lines[i+1]:
                marketing_area = line.strip()
                
            # Check for new section
            if "**OPIS NET TERMINAL" in line:
                if current_section and section_data:
                    processed_data = process_section(section_data, current_section, marketing_area)
                    all_data.extend(processed_data)
                    section_data = []
                
                current_section = line.strip()
                continue
                
            if current_section:
                section_data.append(line)
                
        # Process the last section
        if current_section and section_data:
            processed_data = process_section(section_data, current_section, marketing_area)
            all_data.extend(processed_data)
            
        # Create DataFrame and handle empty results
        if not all_data:
            return None
            
        df = pd.DataFrame(all_data)
        return df
        
    except Exception as e:
        print(f"Error processing document: {str(e)}")
        return None

def process_section(lines, section_name, marketing_area):
    """Process a section of the OPIS file."""
    processed_data = []
    data_start = 2
    
    # Keep track of the original line number within the section
    for line_idx, line in enumerate(lines[data_start:], start=data_start):
        if not line.strip():
            continue
            
        if any(word in line for word in ["Move", "Date", "Time"]):
            continue
            
        is_summary = line.strip().startswith(('TMNL', 'CONT', 'LOW', 'AVG', 'OPIS', 'FOB'))
        
        try:
            data = parse_line(line, is_summary)
            if data:
                data['section'] = section_name
                data['marketing_area'] = marketing_area
                data['line_number'] = line_idx  # Add line number to the data dictionary
                processed_data.append(data)
        except Exception as e:
            print(f"Error processing line: {line}")
            print(f"Error: {str(e)}")
            
    return processed_data

def parse_line(line, is_summary):
    """Parse a single line of OPIS data."""
    if not line.strip():
        return None
        
    price_positions = [28, 36, 43, 51, 58, 66, 73, 79]
    
    try:
        if not is_summary:
            supplier = line[0:11].strip()
            type_code = line[11:13].strip()
            brand = line[13:18].strip()
            terminal = line[18:28].strip()
        else:
            supplier = line[0:28].strip()
            type_code = ''
            brand = ''
            terminal = ''
        
        values = []
        for i in range(len(price_positions)):
            start_pos = price_positions[i]
            end_pos = price_positions[i+1] if i < len(price_positions)-1 else len(line)
            
            if start_pos >= len(line):
                values.append(None)
                continue
                
            value = line[start_pos:end_pos].strip() if start_pos < len(line) else ''
            values.append(value if value and value != '--' else None)
        
        return {
            'supplier': supplier,
            'type': type_code,
            'brand': brand,
            'terminal': terminal,
            'price1': values[0],
            'move1': values[1],
            'price2': values[2],
            'move2': values[3],
            'price3': values[4],
            'move3': values[5],
            'date': values[6],
            'time': values[7]
        }
        
    except Exception as e:
        print(f"Error parsing line: {line}")
        print(f"Error: {str(e)}")
        return None

class OpisStaging:
    def __init__(self):
        """Initialize the Blob extractor"""
        self.process = psutil.Process(os.getpid())
        self.vendor = 'OPIS'

    def _log_operation(self, message: str, is_error: bool = False, memory: bool = True) -> None:
        """Helper method for consistent logging format"""
        mem = f"[bright_white]{self.process.memory_info().rss / (1024**2):,.0f}MB[/bright_white]" if memory else ""
        vendor_str = f" [reverse]{self.vendor}[/reverse] " if self.vendor else " " * 7
        
        message = re.sub(r'(\d+(?:\.\d+)?)', r'[#33cc99]\1[/#33cc99]', message)
        
        if is_error:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  [#FF6E6E]{message}[/#FF6E6E]")
        else:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  {message}")

    def process_opis_files(self):
        start_time = time.time()
        blob_manager = BlobStorageManager("jenkins-pricing-historical", "opis")
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
                    content = blob_manager.read_blob(blob.name).decode('utf-8')
                    df = convert_opis_to_df(content)
                    df['blob_name'] = blob.name
                    if df is not None and not df.empty:
                        all_data.append(df)
                    processed_count += 1
                    progress.update(task, advance=1, description=f"Processing: {blob.name[:50]}")
                except Exception as e:
                    error_count += 1
                    self._log_operation(f"Error processing blob {blob.name}: {e}", is_error=True)
        
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            final_df = final_df.sort_values(['blob_name', 'section', 'line_number']).reset_index(drop=True)
            
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
    staging = OpisStaging()
    final_df = staging.process_opis_files()
    if not final_df.empty:
        print(f"Successfully processed {len(final_df)} records")
    else:
        print("No data was processed") 