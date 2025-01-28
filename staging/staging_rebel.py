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
        
        return df
        
    except Exception as e:
        raise

def get_structure_from_df(df):
    # Find the header row that contains 'Terminal'
    header_row = df[df['Column_0'] == 'Terminal'].index[0]
    
    # Get all products (skipping the first two columns which are typically 'Terminal' and 'Product')
    products = []
    product_cols = {}
    
    # Iterate through columns to find products
    for col in df.columns[2:]:  # Start from third column
        product = df.loc[header_row, col]
        if pd.notna(product) and str(product).strip():  # Check if product name exists
            products.append(product)
            product_cols[product] = col
    
    # Get all terminals (they appear in Column_0 after the header row)
    terminals = []
    for idx in range(header_row + 1, len(df)):
        terminal = df.loc[idx, 'Column_0']
        if pd.notna(terminal) and terminal.strip():  # Check if terminal name exists
            # Stop if we hit a non-terminal row (usually contains metadata/footer information)
            if 'price' in str(terminal).lower() or '@' in str(terminal):
                break
            terminals.append(terminal)
    
    return products, terminals, product_cols

def extract_prices_from_df(df):
    price_mapping = {}
    
    # Get the structure dynamically
    products, terminals, product_cols = get_structure_from_df(df)
    
    # Find the header row
    header_row = df[df['Column_0'] == 'Terminal'].index[0]
    
    # Extract prices for each terminal
    for terminal in terminals:
        # Find the row for this terminal
        terminal_rows = df[df['Column_0'] == terminal]
        if terminal_rows.empty:
            continue
            
        terminal_row = terminal_rows.index[0]
        
        # Get prices for each product
        for product, col in product_cols.items():
            price = df.loc[terminal_row, col]
            if pd.notna(price):  # Check if price is not NaN
                try:
                    # Convert to float and format to 3 decimal places
                    price = f"{float(price):.3f}"
                except (ValueError, TypeError):
                    price = ""  # If conversion fails, use empty string
            else:
                price = ""  # Empty string for missing prices
            
            price_mapping[f"{terminal}-{product}"] = price
    
    return price_mapping, products, terminals

def transform_price_data(df):
    # Define the standard output columns
    output_columns = ['Terminal', 'Product', 'Price', 'Effective Datetime', 'Location']
    
    # Create lists to store the transformed data
    transformed_data = []
    
    # Extract prices and structure
    price_mapping, products, terminals = extract_prices_from_df(df)
    
    # Get the effective date from the data
    date_rows = df[df['Column_0'] == 'Date Effective']
    effective_date = None
    if not date_rows.empty:
        date_row = date_rows.iloc[0]
        if pd.notna(date_row['Column_1']):
            effective_date = date_row['Column_1']
    
    # Get location from the data
    location_rows = df[df['Column_0'] == 'Las Vegas']
    location = None
    if not location_rows.empty:
        location = location_rows.iloc[0]['Column_0']
    
    # Create the transformed data structure
    for terminal in terminals:
        for product in products:
            price = price_mapping.get(f"{terminal}-{product}", "")
            
            transformed_data.append({
                'Terminal': terminal,
                'Product': product,
                'Price': price,
                'Effective Datetime': effective_date,
                'Location': location
            })
    
    # Create new DataFrame with transformed data
    transformed_df = pd.DataFrame(transformed_data, columns=output_columns)
    return transformed_df

def format_price_output(df):
    output_lines = []
    
    # Format each row
    for _, row in df.iterrows():
        line = f"{row['Terminal']} {row['Product']} {row['Price']} {row['Effective Datetime']} {row['Location']}"
        output_lines.append(line)
    
    return '\n'.join(output_lines)

def process_price_sheet(input_df):
    try:
        # Transform the data
        transformed_df = transform_price_data(input_df)
        
        # Format the output
        formatted_output = format_price_output(transformed_df)
        
        return formatted_output
        
    except Exception as e:
        raise

def process_html_to_prices(html_path):
    try:
        # Step 1: Extract table from HTML to DataFrame
        raw_df = extract_table_from_html(html_path)
        
        # Step 2: Transform the raw DataFrame to price format
        transformed_df = transform_price_data(raw_df)
        
        return transformed_df
        
    except Exception as e:
        raise

class RebelStaging:
    def __init__(self):
        """Initialize the Blob extractor"""
        self.process = psutil.Process(os.getpid())
        self.vendor = 'Rebel'

    def _log_operation(self, message: str, is_error: bool = False, memory: bool = True) -> None:
        """Helper method for consistent logging format"""
        mem = f"[bright_white]{self.process.memory_info().rss / (1024**2):,.0f}MB[/bright_white]" if memory else ""
        vendor_str = f" [reverse]{self.vendor}[/reverse] " if self.vendor else " " * 7
        
        message = re.sub(r'(\d+(?:\.\d+)?)', r'[#33cc99]\1[/#33cc99]', message)
        
        if is_error:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  [#FF6E6E]{message}[/#FF6E6E]")
        else:
            rprint(f" {'BlobExtract':14} {mem:8} {vendor_str}  ⎹  {message}")

    def process_rebel_files(self):
        start_time = time.time()
        blob_manager = BlobStorageManager("jenkins-pricing-historical", "rebel")
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
                    df = process_html_to_prices(html_content)
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
    staging = RebelStaging()
    final_df = staging.process_rebel_files()
    if not final_df.empty:
        print(f"Successfully processed {len(final_df)} records")
    else:
        print("No data was processed")