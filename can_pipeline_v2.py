from typing import Dict, List
import pandas as pd
from io import StringIO
from utils.blob_operations import BlobStorageManager

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
    
    def _load_cross_reference(self) -> pd.DataFrame:
        """Load cross reference data."""
        return pd.read_csv("Terminal-ProductNames(Sheet1).csv")
    
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
        df.rename(columns={'product': 'Product', 'price': 'Price', 'location': 'Location'}, inplace=True)

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
    cross_reference = pipeline._load_cross_reference()
    canonical_df_test = pipeline._apply_cross_reference(canonical_df)

    return canonical_df, cross_reference, canonical_df_test
    
    # # Save to canonical storage
    # destination_blob_manager = BlobStorageManager("jenkins-pricing-canonical")
    # destination_blob_manager.upload_blob(
    #     blob_name="historical_master.csv",
    #     content_type="csv",
    #     data=canonical_df.to_csv(index=False)
    # )

df, cr, cdf = main()

# select distinct Supplier Location	Product Terminal
df_subset = df[['Supplier', 'Location', 'Product', 'Terminal', 'Brand']].drop_duplicates()
# merge cr on Supplier, Product Description = Product, Terminal (Old) = Terminal
df_subset = cr.merge(df_subset, left_on=['Supplier', 'Product Description', 'Terminal (Old)', 'Location'], right_on=['Supplier', 'Product', 'Terminal', 'Location'], how='left')
# rearange columns
df_subset = df_subset[['Supplier', 'Location', 'Terminal', 'Terminal (Old)', 'Product', 'Product Description', 'Supply Area', 'Product Code', 'Terminal (New)', 'Brand', 'Product Group', 'Alternate Supplier/Account']]

df_subset.to_csv("df_subset.csv", index=False)