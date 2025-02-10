from typing import Dict, List
import pandas as pd
from io import StringIO
from utils.blob_operations import BlobStorageManager
import re

# Constants
SUPPLIERS = [
    'bbenergy', 
    'bigwest',
    'bradhall',
    'chevron',
    'chevron-tca',
    'eprod', 
    'kotaco',
    'marathon', 
    # 'marathon-tca',
    'musket',
    'offen',
    'opis',
    'rebel', 
    'shell', 
    'sinclair', 
    'sunoco', 
    'tartan', 
    'valero'
]

ENCODINGS = ['utf-8', 'utf-16', 'iso-8859-1', 'cp1252']
STANDARD_COLUMNS = ['Supplier', 'Location', 'Terminal', 'Product', 'Brand', 'Price', 'Datetime', 'Date', 'Time']

class CanonicalPipeline:
    """
    Pipeline for processing vendor data files into a canonical format.
    """
    
    def __init__(self):
        self.vendor_dfs = {}
        
    def load_vendor_data(self) -> None:
        """Load all vendor data from blob storage."""
        for vendor in SUPPLIERS:
            self.vendor_dfs[vendor] = self._load_single_vendor(vendor)
            
    def _load_single_vendor(self, vendor: str) -> pd.DataFrame:
        """Load data for a single vendor with multiple encoding attempts."""
        blob_manager = BlobStorageManager(f"jenkins-pricing-staging/{vendor}")
        data = blob_manager.read_blob(f"{vendor}_historical_master.csv")
        
        for encoding in ENCODINGS:
            try:
                data_str = data.decode(encoding)
                for separator in [',', ';', '\t']:
                    try:
                        return pd.read_csv(StringIO(data_str), sep=separator)
                    except:
                        continue
            except UnicodeDecodeError:
                continue
        return pd.DataFrame()

    def process_all_vendors(self) -> pd.DataFrame:
        """Process all vendor dataframes and combine into canonical format."""
        processed_dfs = []
        
        # Process each vendor
        vendor_processors = {
            'bbenergy': self._process_bbenergy,
            'bigwest': self._process_bigwest,
            'bradhall': self._process_bradhall,
            'chevron': self._process_chevron,
            # 'chevron-tca': self._process_chevron_tca,
            # 'eprod': self._process_eprod,
            'kotaco': self._process_kotaco,
            'marathon': self._process_marathon,
            # 'marathon-tca': self._process_marathon_tca,
            'musket': self._process_musket,
            'offen': self._process_offen,
            # 'opis': self._process_opis,
            'rebel': self._process_rebel,
            'shell': self._process_shell,
            'sinclair': self._process_sinclair,
            'sunoco': self._process_sunoco,
            'tartan': self._process_tartan,
            'valero': self._process_valero
        }
        
        for vendor, processor in vendor_processors.items():
            if vendor in self.vendor_dfs and self.vendor_dfs[vendor] is not None:
                processed_df = processor(self.vendor_dfs[vendor])
                if processed_df is not None:
                    processed_dfs.append(processed_df)
        
        combined_df = pd.concat(processed_dfs, ignore_index=True)
        combined_df = combined_df.drop_duplicates()
        
        combined_df = combined_df.sort_values(by=['Supplier', 'Location', 'Terminal', 'Product', 'Datetime'])
        combined_df['Price_Yesterday'] = combined_df.groupby(['Supplier', 'Location', 'Terminal', 'Product'])['Price'].shift(1)
        combined_df['Change'] = combined_df['Price'] - combined_df['Price_Yesterday']
        combined_df.drop(columns=['Price_Yesterday'], inplace=True)
        
        return combined_df

    def _standardize_datetime(self, df: pd.DataFrame, date_col: str, time_col: str) -> pd.DataFrame:
        """Standardize datetime columns across vendors."""
        df['Date'] = pd.to_datetime(df[date_col], format='mixed').dt.strftime('%Y-%m-%d')
        df['Time'] = pd.to_datetime(df[time_col], format='mixed').dt.strftime('%H:%M:%S')
        df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
        return df
    
    # def _load_cross_reference(self) -> pd.DataFrame:
    #     """Load cross reference data."""
    #     return pd.read_csv("Terminal-ProductNames(Sheet1).csv")
    
    def _apply_cross_reference(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply cross reference data to vendor data."""
        cross_reference = self._load_cross_reference()

        # merge on supplier, product description = product, and terminal (old) = terminal
        df = df.merge(cross_reference, left_on=['Supplier', 'Product', 'Terminal'], right_on=['Supplier', 'Product Description', 'Terminal (Old)'], how='left')

        return df

    def _process_bbenergy(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process BBEnergy data into canonical format."""
        df = df.copy()
        df.rename(columns={'product': 'Product Description', 'price': 'Price'}, inplace=True)
        
        # Standardize dates
        df = self._standardize_datetime(df, 'date', 'time')
        
        # Process location
        df['Location'] = df['location'].str.split('-').str[0]
        df['Terminal'] = df['location'].str.split('-').str[1]
        df['Supplier'] = 'BBEnergy'
        df['Brand'] = 'Unbranded'
        
        product_codes = {
            "B5": "DSL#2 B5",
            "B5-Red": "RED#2 B5",
            "CARB E10-Prem": "CARB 91 E10",
            "CARB E10-Prem TT": "CARB 91 E10 TT",
            "CARB E10-Unl": "CARB 87 E10",
            "CARB E10-Unl TT": "CARB 87 E10 TT",
            "CARB ULSD": "CARB DSL#2",
            "CARB ULSD-Red": "CARB RED#2",
            "CBG E10-Prem": "CBG 91 E10",
            "CBG E10-Prem TT": "CBG 91 E10 TT",
            "CBG E10-Unl": "CBG 87 E10",
            "CBG E10-Unl TT": "CBG 87 E10 TT",
            "E10-Prem": "92 E10",
            "E10-Prem TT": "92 E10 TT",
            "E10-Unl": "87 E10",
            "E10-Unl TT": "87 E10 TT",
            "RFG E10-Unl": "RFG 85 E10",
            "RFG E10-Unl TT": "RFG 85 E10",
            "ULSD": "DSL#2",
            "ULSD Winterized": "DSL#2 CFI",
            "ULSD-Red": "RED#2",
            "ULSD-Red Winteriz": "RED#2 CFI",
            "UL2 LED DYED": "RED#2",
            "ULSD LED": "DSL#2",
            "ULSD LED-Red": "RED#2",
        }

        df['Product'] = df['Product Description'].map(product_codes)

        return df[STANDARD_COLUMNS]
    
    def _process_bigwest(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process BigWest data into canonical format."""
        df = df.copy()
        df.rename(columns={'product': 'Product', 'price': 'Price', 'location': 'Location'}, inplace=True)
        
        # Standardize dates
        df = self._standardize_datetime(df, 'date', 'time')
        
        # Process location
        df['Terminal'] = ''
        df['Supplier'] = 'BigWest'
        df['Brand'] = ''
        return df[STANDARD_COLUMNS]
    
    def _process_bradhall(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process BradHall data into canonical format."""
        df = df.copy()
        df.rename(columns={'product': 'Product', 'price': 'Price', 'terminal_code': 'Terminal', 'marketing_area': 'Location'}, inplace=True)

        df = self._standardize_datetime(df, 'date', 'time')
        
        df['Supplier'] = 'BradHall'
        df['Location'] = ''
        df['Terminal'] = ''
        df['Brand'] = ''
        return df[STANDARD_COLUMNS]
    
    def _process_chevron(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process Chevron data into canonical format."""
        df = df.copy()
        
        df = self._standardize_datetime(df, 'Effective_Date', 'Effective_Date')
        
        df['Supplier'] = 'Chevron'
        df['Location'] = ''
        df['Brand'] = ''

        return df[STANDARD_COLUMNS]
    
    def _process_eprod(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process Eprod data into canonical format."""
        df = df.copy()
        
        df = self._standardize_datetime(df, 'effective_datetime', 'effective_datetime')
        
        df['Supplier'] = 'Eprod'
        df['Location'] = df['location'].str.split(' ').str[0]
        df['Terminal'] = df['location'].str.split(' ').str[1]
        df['Brand'] = ''
        return df[STANDARD_COLUMNS]
    
    def _process_kotaco(self, df: pd.DataFrame) -> pd.DataFrame:    
        """Process Kotaco data into canonical format."""
        df = df.copy()
        
        df = self._standardize_datetime(df, 'Effective_Date', 'Effective_Date')
        
        df['Supplier'] = 'Kotaco' + '-' + df['Supplier']
        df['Location'] = ''
        df['Brand'] = ''
        return df[STANDARD_COLUMNS]
    
    def _process_marathon(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process Marathon data into canonical format."""
        df = df.copy()

        df.rename(columns={'product': 'Product', 'price': 'Price', 'terminal': 'Terminal'}, inplace=True)

        df = self._standardize_datetime(df, 'effective_datetime', 'effective_datetime')

        df['Supplier'] = 'Marathon'
        df['Location'] = ''
        df['Brand'] = ''

        return df[STANDARD_COLUMNS]
    
    # def _process_marathon_tca(self, df: pd.DataFrame) -> pd.DataFrame:
    #     """Process Marathon TCA data into canonical format."""
    #     df = df.copy()

    #     df = self._standardize_datetime(df, 'Effective_Date', 'Effective_Date')

    #     return df[STANDARD_COLUMNS]

    def _process_musket(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process Musket data into canonical format."""
        df = df.copy()
        
        df.rename(columns={'product': 'Product', 'price': 'Price'}, inplace=True)

        df = self._standardize_datetime(df, 'effective_datetime', 'effective_datetime')

        df['Supplier'] = 'Musket'
        df['Location'] = df['location'].str.split('-').str[0]
        df['Terminal'] = df['location'].str.split('-').str[1]
        df['Brand'] = ''

        return df[STANDARD_COLUMNS]
    
    def _process_opis(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process Opis data into canonical format."""
        df = df.copy()

        # Extract product group from section column
        df['Product_Group'] = df['section'].str.extract(r'\*\*OPIS NET TERMINAL(.*?)PRICES\*\*', expand=False).str.strip()

        # Filter rows based on type
        df = df[
            (df['type'].isin(['u', 'b'])) | 
            (df['type'].isna()) |
            (df['type'].isnull()) |
            (df['type'] == '') |
            (df['type'].str.strip() == '') |
            (pd.isna(df['type']))
        ]

        # Remove OPIS and RENO, NV from supplier
        df = df[~df['supplier'].str.contains('OPIS')]
        df = df[~df['supplier'].str.contains('RENO, NV')]

        # Process supplier and dates
        df['Report_Date'] = df['marketing_area'].str.extract(r'(\d{4}-\d{2}-\d{2})')
        df['Year'] = df['Report_Date'].str[:4]
        df['Agg_Date'] = df['supplier'].str.extract(r'(\d{2}/\d{2})')
        df['Supplier'] = df['supplier'].apply(lambda x: 
            x[:-6] if isinstance(x, str) and 'CONT' in x 
            else x
        )

        product_group_mapping = {
            'CBG ETHANOL (10%)': ['UNL', 'MID', 'PRE'],
            'CBG ETHANOL (10%) TOP TIER': ['UNL', 'MID', 'PRE'],
            'CBOB ETHANOL(10%)': ['UNL', 'MID', 'PRE'],
            'CBOB ETHANOL(10%) TOP TIER': ['UNL', 'MID', 'PRE'],
            'CONV. CLEAR': ['UNL', 'MID', 'PRE'],
            'E-55': ['UNL'],
            'E-70': ['UNL'],
            'E-85': ['UNL'],
            'LOW SULFUR KEROSENE': ['KERO', 'KERO RD', 'KERO NRLM'],
            'RENEWABLE R99 ULTRA LOW SULFUR DISTILLATE': ['No.2'],
            'SPECIALTY DISTILLATE': ['JET', 'MARINE'],
            'ULTRA LOW SULFUR DISTILLATE': ['No.2', 'No.1', 'Pre'],
            'ULTRA LOW SULFUR RED DYE DISTILLATE': ['No.2', 'No.1', 'Pre'],
            'ULTRA LOW SULFUR RED DYE WINTER DISTILLATE': ['No.2', 'No.1', 'Pre'],
            'ULTRA LOW SULFUR WINTER DISTILLATE': ['No.2', 'No.1', 'Pre'],
            'WHOLESALE B0-5 SME BIODIESEL': ['ULS No.2', 'ULS2 RD'],
            'WHOLESALE B20 SME BIODIESEL': ['ULS No.2'],
            'WHOLESALE B5 SME BIODIESEL': ['ULS No.2', 'ULS2 RD']
        }

        df['Product'] = df['Product_Group'].map(product_group_mapping)

        def determine_date(row):
            """
            Determine the date based on product type and conditions.
            Returns date in YYYY-MM-DD format.
            """
            # If Agg_Date exists, use it with the year
            if pd.notna(row['Agg_Date']) and pd.notna(row['Year']):
                month, day = row['Agg_Date'].split('/')
                return f"{row['Year']}-{month}-{day}"
            
            # If type is missing, use marketing_area date
            if pd.isna(row['type']):
                return row['Report_Date']
                
            products = product_group_mapping.get(row['Product_Group'], [])
            
            # Extract MM/DD from string and combine with year
            def extract_and_format_date(date_str, year):
                if pd.isna(date_str) or pd.isna(year):
                    return None
                pattern = r'(\d{2})/(\d{2})'
                match = re.search(pattern, str(date_str))  # Convert to string to handle non-string inputs
                if match:
                    month = match.group(1)
                    day = match.group(2)
                    return f"{year}-{month}-{day}"
                return None

            # Special cases for ULS No.2
            if products == ['ULS No.2']:
                date = extract_and_format_date(row['move1'], row['Year'])
                return date if date is not None else extract_and_format_date(row['price2'], row['Year'])
            elif products == ['ULS No.2', 'ULS2 RD']:
                for field in ['price3', 'price2', 'move1']:
                    date = extract_and_format_date(row[field], row['Year'])
                    if date is not None:
                        return date
            
            # General cases based on number of products
            if len(products) == 3:
                return extract_and_format_date(row['date'], row['Year'])
            elif len(products) == 2:
                return extract_and_format_date(row['price3'], row['Year'])
            elif len(products) == 1:
                return extract_and_format_date(row['price2'], row['Year'])
                
            return None

        df['Date'] = df.apply(determine_date, axis=1)

        def extract_time_from_move3(move_val, price_val=None, date_val=None):
            """
            Extract time from move3, using price3 or date as backup sources.
            
            Args:
                move_val: Value from move3 column
                price_val: Value from price3 column (for :mm case)
                date_val: Value from date column (for hh:mm:s case)
                
            Returns:
                Formatted time string (HH:MM) or None if no valid time found
            """
            if pd.isna(move_val):
                return None
                
            move_str = str(move_val)
            
            # Case 1: Check for hh:mm:s format
            special_format_match = re.search(r'(\d{1,2}):(\d{2}):(\d{1})', move_str)
            if special_format_match and pd.notna(date_val):
                try:
                    hour = int(special_format_match.group(1))
                    minutes = int(special_format_match.group(2))
                    last_digit = str(date_val)[-1]
                    if 0 <= hour <= 23 and 0 <= minutes <= 59:
                        return f"{hour:02d}:{minutes:02d}"
                except (ValueError, TypeError, IndexError):
                    pass

            # Case 2: Check for standard hh:mm format
            full_time_match = re.search(r'(\d{1,2}):(\d{2})', move_str)
            if full_time_match:
                try:
                    hour = int(full_time_match.group(1))
                    minutes = int(full_time_match.group(2))
                    if 0 <= hour <= 23 and 0 <= minutes <= 59:
                        return f"{hour:02d}:{minutes:02d}"
                except ValueError:
                    pass
            
            # Case 3: Check for :mm and use price3 for hour
            minutes_match = re.search(r':(\d{2})', move_str)
            if minutes_match and pd.notna(price_val):
                try:
                    price_str = str(price_val)
                    price_digits_match = re.search(r'^(\d{1,2})', price_str)
                    if price_digits_match:
                        hour = int(price_digits_match.group(1))
                        minutes = int(minutes_match.group(1))
                        if 0 <= hour <= 23 and 0 <= minutes <= 59:
                            return f"{hour:02d}:{minutes:02d}"
                except (ValueError, TypeError):
                    pass
                    
            return None

        def extract_time_from_move2(move_val, price_val=None):
            """
            Extract time from move2 and price2 columns.
            
            Args:
                move_val: Value from move2 column
                price_val: Value from price2 column (for :mm case)
                
            Returns:
                Formatted time string (HH:MM) or None if no valid time found
            """
            if pd.isna(move_val):
                return None
                
            move_str = str(move_val)
            
            # Try to find full time pattern (hh:mm)
            full_time_match = re.search(r'(\d{1,2}):(\d{2})', move_str)
            if full_time_match:
                try:
                    hour = int(full_time_match.group(1))
                    minutes = int(full_time_match.group(2))
                    if 0 <= hour <= 23 and 0 <= minutes <= 59:
                        return f"{hour:02d}:{minutes:02d}"
                except ValueError:
                    return None
                
            # Handle special case where only :mm is present
            minutes_match = re.search(r':(\d{2})', move_str)
            if minutes_match and pd.notna(price_val):
                try:
                    price_str = str(price_val)
                    price_digits_match = re.search(r'^(\d{1,2})', price_str)
                    if price_digits_match:
                        hour = int(price_digits_match.group(1))
                        minutes = int(minutes_match.group(1))
                        if 0 <= hour <= 23 and 0 <= minutes <= 59:
                            return f"{hour:02d}:{minutes:02d}"
                except (ValueError, TypeError):
                    return None
                    
            return None

        def process_time(df):
            """
            Process time column based on multiple sources.
            
            Args:
                df: pandas DataFrame containing required columns
                
            Returns:
                Series containing processed times
            """
            def determine_time(row):
                # Check supplier condition first
                if pd.notna(row.get('supplier')) and any(key in str(row['supplier']).upper() for key in ['TMNL', 'RETAIL', 'CONT', 'FOB']):
                    if pd.notna(row.get('marketing_area')):
                        time_match = re.search(r'(\d{2}):(\d{2}):(\d{2})', str(row['marketing_area']))
                        if time_match:
                            return time_match.group(0)
                
                # If time column has valid value, use it
                if pd.notna(row.get('time')):
                    return row['time']
                
                # Try move3 with all its special cases
                time_from_move3 = extract_time_from_move3(row.get('move3'), row.get('price3'), row.get('date'))
                if time_from_move3:
                    return time_from_move3
                
                # Try move2 with price2 as backup
                time_from_move2 = extract_time_from_move2(row.get('move2'), row.get('price2'))
                if time_from_move2:
                    return time_from_move2
                
                # if supplier contains Tartan then time is 18:00
                if 'TARTAN' in str(row['supplier']).upper():
                    return '18:00'

                return None

            # Verify required columns exist
            required_columns = ['time', 'move2', 'move3', 'price2', 'price3', 'date', 'supplier', 'marketing_area']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # Return only the processed time series
            return df.apply(determine_time, axis=1)

        df['Time'] = process_time(df)
        df['Date'] = pd.to_datetime(df['Date'], format='mixed').dt.strftime('%Y-%m-%d')
        df['Time'] = pd.to_datetime(df['Time'], format='mixed').dt.strftime('%H:%M:%S')
        df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])

        def extract_location(row):
            """
            Extract city and state from marketing_area when supplier contains specific keywords.
            
            Args:
                row: DataFrame row containing 'supplier' and 'marketing_area' columns
                
            Returns:
                tuple: (city, state) or (None, None) if no match found
            """
            # Check if supplier contains any of the keywords
            keywords = ['TMNL', 'FOB', 'RETAIL', 'CONT']
            
            if not pd.isna(row['supplier']) and any(key in str(row['supplier']).upper() for key in keywords):
                if pd.isna(row['marketing_area']):
                    return None
                    
                # Extract location pattern: text before comma, then space and two letters
                pattern = r'^([^,]+),\s*([A-Z]{2})'
                match = re.search(pattern, str(row['marketing_area']))
                
                if match:
                    # if match then combine the city and state into a single column
                    return f"{match.group(1).strip()}, {match.group(2)}"
                    
            return None

        df['location'] = df.apply(extract_location, axis=1)
        df.rename(columns={'type': 'Brand', 'brand': 'location_code'}, inplace=True)
        df = df[['Supplier', 'location', "location_code", 'terminal', 'Product_Group', 'Product', 'price1', 'price2', 'price3', 'move1', 'move2', 'move3', 'Date', 'Time', 'Datetime', 'Brand', 'line_number', 'blob_name']]

        def assign_prices_optimized(df):
            """
            Assign prices to their respective products using vectorized operations.
            Preserves all original columns from the input DataFrame.
            
            Args:
                df: DataFrame containing the product and price information
                
            Returns:
                DataFrame with prices assigned to individual products
            """
            print(f"Processing {len(df)} rows...")
            
            # Create a copy to avoid modifying the original
            df = df.copy()
            
            # Convert string lists to actual lists
            df['Product'] = df['Product'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
            
            # Initialize list to store the expanded data
            expanded_data = []
            
            # Create mapping for price and move columns
            price_cols = {0: 'price1', 1: 'price2', 2: 'price3'}
            move_cols = {0: 'move1', 1: 'move2', 2: 'move3'}
            
            # Iterate through DataFrame with simple progress logging
            total_rows = len(df)
            for idx, row in enumerate(df.iterrows(), 1):
                if idx % 100 == 0:  # Log every 100 rows
                    print(f"Processing row {idx}/{total_rows} ({(idx/total_rows*100):.1f}%)")
                    
                _, row = row  # Unpack the row tuple
                try:
                    # For each product in the list, create a new row
                    for i, product in enumerate(row['Product']):
                        new_row = row.copy()
                        new_row['Product'] = product
                        new_row['Price'] = row[price_cols.get(i, 'price1')]
                        new_row['Price_Move'] = row[move_cols.get(i, 'move1')]
                        expanded_data.append(new_row.to_dict())
                        
                except Exception as e:
                    print(f"Error processing row {idx}: {e}")
                    print(f"Row data: {row}")
                    continue
            
            # Create final dataframe
            result_df = pd.DataFrame(expanded_data)
            
            # Drop the original price and move columns
            columns_to_drop = ['price1', 'price2', 'price3', 'move1', 'move2', 'move3']
            result_df = result_df.drop(columns=columns_to_drop)
            
            print("Processing complete!")
            print(f"Input rows: {len(df)}")
            print(f"Output rows: {len(result_df)}")

            # drop rows where Price is None
            result_df = result_df[result_df['Price'].notna()]

            return result_df
        
        df = assign_prices_optimized(df)

        return df[STANDARD_COLUMNS]

    def _process_offen(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process Offen data into canonical format."""
        df = df.copy()

        df = df[~df['Terminal'].isin(['Terms Net 10 Days via EFT or ACH', 'Above prices are subject to midday changes and do not inculde any tax or freight'])]

        df['Datetime'] = pd.to_datetime(df['Effective'].str.split(' - ').str[0], format='%m/%d/%Y %I:%M %p')

        df['Date'] = df['Datetime'].dt.strftime('%Y-%m-%d')
        df['Time'] = df['Datetime'].dt.strftime('%H:%M:%S')

        df['Supplier'] = 'Offen'
        df['Location'] = ''
        df['Brand'] = ''
        return df[STANDARD_COLUMNS]
    
    def _process_rebel(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process Rebel data into canonical format."""
        df = df.copy()

        df = df[~df['Terminal'].isin([
            'Cyndi Maurycy|Wholesale Fuels Specialist', 
            'Office:  (702) 382-5866', 
            'Rebel Oil Company dba ROC', 
            'Cell: (725) 377-3598',
            '10650 W. Charleston Blvd., Suite 100Las Vegas, NV 89135',
            'Office:  (702) 382-5866',
            'UT'
        ])]

        df['Date_Int'] = df['Effective Datetime'].str.split(r'[-:]').str[0]
        df['Date'] = pd.to_datetime(df['Date_Int'], format='mixed').dt.strftime('%Y-%m-%d')
        df['Time'] = '00:00:00'

        df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])

        df['Supplier'] = 'Rebel'
        df['Location'] = ''
        df['Brand'] = ''
        return df[STANDARD_COLUMNS]
    
    def _process_shell(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process Shell data into canonical format."""
        df = df.copy()

        df.rename(columns={'Product Name': 'Product'}, inplace=True)

        df = self._standardize_datetime(df, 'Effective Date', 'Effective Date')

        df['Supplier'] = 'Shell'
        df['Location'] = df['Terminal Name'].str.split('-').str[0]
        df['Terminal'] = df['Terminal Name'].str.split('-').str[1]
        df['Brand'] = ''

        return df[STANDARD_COLUMNS]
    
    def _process_sinclair(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process Sinclair data into canonical format."""
        df = df.copy()
        
        df.rename(columns={'product': 'Product', 'price': 'Price'}, inplace=True)

        df = self._standardize_datetime(df, 'effective_datetime', 'effective_datetime')

        df['Location'] = df['location'].str.split('-').str[0]
        df['Terminal'] = df['location'].str.split('-').str[1]
        df['Supplier'] = df['supplier']
        df['Brand'] = df['brand']

        return df[STANDARD_COLUMNS]
    
    def _process_sunoco(self, df: pd.DataFrame) -> pd.DataFrame:    
        """Process Sunoco data into canonical format."""
        df = df.copy()
        
        df.rename(columns={'product': 'Product', 'price': 'Price'}, inplace=True)

        df = self._standardize_datetime(df, 'effective_datetime', 'effective_datetime') 

        df['Supplier'] = 'Sunoco'
        df['Location'] = df['location'].str.split('-').str[0]
        df['Terminal'] = df['location'].str.split('-').str[1]

        df['Brand'] = ''

        return df[STANDARD_COLUMNS] 
    
    def _process_tartan(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process Tartan data into canonical format."""
        df = df.copy()  
        
        df['Date'] = pd.to_datetime(df['Effective Date'], format='mixed').dt.strftime('%Y-%m-%d')
        df['Time'] = '00:00:00'
        df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])

        df['Supplier'] = 'Tartan'
        df['Location'] = ''
        df['Brand'] = ''
        def cascade_fill_location_and_terminal(df):

            df['Terminal'] = None
            current_location = None
            current_terminal = None

            main_locations = {'Las Vegas', 'Salt Lake'}
            
            for idx in range(len(df)):
                loc = df.iloc[idx]['Location']
                
                if pd.isna(loc):
                    if current_location is not None:
                        df.at[idx, 'Location'] = current_location
                    if current_terminal is not None:
                        df.at[idx, 'Terminal'] = current_terminal
                else:
                    if loc in main_locations:
                        current_location = loc
                        current_terminal = None
                        df.at[idx, 'Terminal'] = None
                    else:
                        if current_location is not None:
                            df.at[idx, 'Location'] = current_location
                        current_terminal = loc
                        df.at[idx, 'Terminal'] = current_terminal
                        
            return df

        df = cascade_fill_location_and_terminal(df)
        df['Terminal'] = df['Terminal'].fillna(df['Location'])

        return df[STANDARD_COLUMNS]
    
    def _process_valero(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process Valero data into canonical format."""
        df = df.copy()

        df.rename(columns={'product': 'Product', 'price': 'Price'}, inplace=True)

        df = self._standardize_datetime(df, 'effective_datetime', 'effective_datetime')

        df['Price'] = df['Price'] / 100

        df['Supplier'] = 'Valero'
        df['Location'] = df['terminal'].str.split(' ').str[0] + ' ' + df['terminal'].str.split(' ').str[1]
        df['Terminal'] = df['terminal'].str.split(' ').str[3] + ' ' + df['terminal'].str.split(' ').str[4] + ' ' + df['terminal'].str.split(' ').str[5]
        df['Brand'] = ''

        return df[STANDARD_COLUMNS]
    
    def _process_chevron_tca(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process Chevron TCA data into canonical format."""
        df = df.copy()

        df = self._standardize_datetime(df, 'Effective_Date', 'Effective_Date')

        df['Supplier'] = 'Chevron TCA'
        df['Location'] = ''
        df['Brand'] = ''
        return df[STANDARD_COLUMNS]

def main():
    pipeline = CanonicalPipeline()
    pipeline.load_vendor_data()
    canonical_df = pipeline.process_all_vendors()
    # cross_reference = pipeline._load_cross_reference()
    # canonical_df_test = pipeline._apply_cross_reference(canonical_df)

    # return canonical_df, cross_reference, canonical_df_test
    return canonical_df
    
    # # Save to canonical storage
    # destination_blob_manager = BlobStorageManager("jenkins-pricing-canonical")
    # destination_blob_manager.upload_blob(
    #     blob_name="historical_master.csv",
    #     content_type="csv",
    #     data=canonical_df.to_csv(index=False)
    # )

# df, cr, cdf = main()
df = main()

# select distinct Supplier Location	Product Terminal
df_subset = df[['Supplier', 'Location', 'Product', 'Terminal', 'Brand']].drop_duplicates()
# merge cr on Supplier, Product Description = Product, Terminal (Old) = Terminal
# df_subset = cr.merge(df_subset, left_on=['Supplier', 'Product Description', 'Terminal (Old)', 'Location'], right_on=['Supplier', 'Product', 'Terminal', 'Location'], how='left')
# rearange columns
df_subset = df_subset[['Supplier', 'Location', 'Terminal', 'Terminal (Old)', 'Product', 'Product Description', 'Supply Area', 'Product Code', 'Terminal (New)', 'Brand', 'Product Group', 'Alternate Supplier/Account']]

df_subset.to_csv("df_subset.csv", index=False)