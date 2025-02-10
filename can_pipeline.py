from utils.blob_operations import BlobStorageManager
import pandas as pd
from io import StringIO
import re
import ast

suppliers = [
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
    'valero', 
]

vendor_dfs = {}
for vendor in suppliers:
    blob_manager = BlobStorageManager(f"jenkins-pricing-staging/{vendor}")
    data = blob_manager.read_blob(f"{vendor}_historical_master.csv")

    # Try different encodings
    encodings = ['utf-8', 'utf-16', 'iso-8859-1', 'cp1252']
    df = None
    last_error = None

    for encoding in encodings:
        try:
            data_str = data.decode(encoding)
            # Try different CSV parsing options
            try:
                df = pd.read_csv(StringIO(data_str))
                # print("Used encoding: ", encoding)
                break
            except:
                try:
                    df = pd.read_csv(StringIO(data_str), sep=';')
                    # print("Used sep: ;")
                    break
                except:
                    try:
                        df = pd.read_csv(StringIO(data_str), sep='\t')
                        # print("Used sep: \t")
                        break
                    except Exception as e:
                        last_error = e
                        continue
        except UnicodeDecodeError:
            continue

    vendor_dfs[vendor] = df

#------------------------------------------------------------------------------------------------------------------
def process_bbenergy(vendor_dfs):
    df_bbenergy = vendor_dfs['bbenergy']

    # make product and price column names uppercase
    df_bbenergy.rename(columns={'product': 'Product', 'price': 'Price'}, inplace=True)

    # make date yyyy-mm-dd
    df_bbenergy['Date'] = pd.to_datetime(df_bbenergy['date'], format='%m/%d/%y').dt.strftime('%Y-%m-%d')

    # make time hh:mm:ss
    df_bbenergy['Time'] = pd.to_datetime(df_bbenergy['time'], format='%H:%M').dt.strftime('%H:%M:%S')

    # create datetime column
    df_bbenergy['Datetime'] = pd.to_datetime(df_bbenergy['Date'] + ' ' + df_bbenergy['Time'])

    # seperate location column by - into location and terminal 
    df_bbenergy['Location'] = df_bbenergy['location'].str.split('-').str[0]
    df_bbenergy['Terminal'] = df_bbenergy['location'].str.split('-').str[1]

    # assign supplier
    df_bbenergy['Supplier'] = 'BBEnergy'

    # select columns
    df_bbenergy = df_bbenergy[['Supplier', 'Location', 'Terminal', 'Product', 'Price', 'Datetime', 'Date', 'Time']]
    return df_bbenergy

df_bbenergy = process_bbenergy(vendor_dfs)
# select distinct product
df_bbenergy[['Location', 'Terminal', 'Product']].drop_duplicates().sort_values(by=['Product'])

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

#apply product codes to df_bbenergy
df_bbenergy['Product Code'] = df_bbenergy['Product'].map(product_codes)

#------------------------------------------------------------------------------------------------------------------
## bigwest
df_bigwest = vendor_dfs['bigwest']

# create datetime column
df_bigwest['Datetime'] = pd.to_datetime(df_bigwest['date'] + ' ' + df_bigwest['time'])

# assign supplier
df_bigwest['Supplier'] = 'BigWest'

# make product and price column names uppercase
df_bigwest.rename(columns={'product': 'Product', 'price': 'Price', 'date': 'Date', 'time': 'Time', 'location': 'Location'}, inplace=True)

df_bigwest['Terminal'] = ''

# select columns
df_bigwest = df_bigwest[['Supplier', 'Location', 'Terminal', 'Product', 'Price', 'Datetime', 'Date', 'Time']]

df_bigwest

#------------------------------------------------------------------------------------------------------------------
## bradhall
df_bradhall = vendor_dfs['bradhall']

# create datetime column
df_bradhall['Datetime'] = pd.to_datetime(df_bradhall['date'] + ' ' + df_bradhall['time'])

# assign supplier
df_bradhall['Supplier'] = 'BradHall'

# make product and price column names uppercase
df_bradhall.rename(columns={'product': 'Product', 'price': 'Price', 'date': 'Date', 'time': 'Time', 'location': 'Location'}, inplace=True)

df_bradhall['Terminal'] = ''

# select columns
df_bradhall = df_bradhall[['Supplier', 'Location', 'Terminal', 'Product', 'Price', 'Datetime', 'Date', 'Time']]

#------------------------------------------------------------------------------------------------------------------
## chevron
df_chevron = vendor_dfs['chevron']

# create date and time columns from Effective_Date
df_chevron['Date'] = pd.to_datetime(df_chevron['Effective_Date'], format='mixed').dt.strftime('%Y-%m-%d')
df_chevron['Time'] = pd.to_datetime(df_chevron['Effective_Date'], format='mixed').dt.strftime('%H:%M:%S')

# create datetime column
df_chevron['Datetime'] = pd.to_datetime(df_chevron['Date'] + ' ' + df_chevron['Time'])

# assign supplier
df_chevron['Supplier'] = 'Chevron'

df_chevron['Location'] = ''

# select columns
df_chevron = df_chevron[['Supplier', 'Location', 'Terminal', 'Product', 'Price', 'Datetime', 'Date', 'Time']]

#------------------------------------------------------------------------------------------------------------------
# TODO: figure out what total_price is
## eprod
df_eprod = vendor_dfs['eprod']

# create Date and Time from effective_datetime
df_eprod['Date'] = pd.to_datetime(df_eprod['effective_datetime'], format='mixed').dt.strftime('%Y-%m-%d')
df_eprod['Time'] = pd.to_datetime(df_eprod['effective_datetime'], format='mixed').dt.strftime('%H:%M:%S')

# create datetime column
df_eprod['Datetime'] = pd.to_datetime(df_eprod['Date'] + ' ' + df_eprod['Time'])

# assign supplier
df_eprod['Supplier'] = 'Eprod'

# seperate location column by - into location and terminal 
df_eprod['Location'] = df_eprod['location'].str.split(' ').str[0]
df_eprod['Terminal'] = df_eprod['location'].str.split(' ').str[1]

# select columns
# df_eprod = df_eprod[['Supplier', 'Location', 'Terminal', 'Product', 'Price', 'Datetime', 'Date', 'Time']]

#------------------------------------------------------------------------------------------------------------------
## kotaco
df_kotaco = vendor_dfs['kotaco']

# create Date and Time from effective_datetime
df_kotaco['Date'] = pd.to_datetime(df_kotaco['Effective_Date'], format='mixed').dt.strftime('%Y-%m-%d')
df_kotaco['Time'] = pd.to_datetime(df_kotaco['Effective_Date'], format='mixed').dt.strftime('%H:%M:%S')

# create datetime column
df_kotaco['Datetime'] = pd.to_datetime(df_kotaco['Date'] + ' ' + df_kotaco['Time'])

# assign supplier by adding 'Kotaco' to the end of the supplier column
df_kotaco['Supplier'] = 'KOTACO' + '-' + df_kotaco['Supplier']

df_kotaco['Location'] = ''

# select columns
df_kotaco = df_kotaco[['Supplier', 'Location', 'Terminal', 'Product', 'Price', 'Datetime', 'Date', 'Time']]

#------------------------------------------------------------------------------------------------------------------
## musket
def process_musket_df(vendor_dfs):
    df = vendor_dfs['musket']
    
    # make product and price column names uppercase
    df.rename(columns={'product': 'Product', 'price': 'Price'}, inplace=True)

    # create Date and Time from effective_datetime
    df['Date'] = pd.to_datetime(df['effective_datetime'], format='mixed').dt.strftime('%Y-%m-%d')
    df['Time'] = pd.to_datetime(df['effective_datetime'], format='mixed').dt.strftime('%H:%M:%S')

    # create datetime column
    df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])

    # seperate location column by - into location and terminal 
    df['Location'] = df['location'].str.split('-').str[0]
    df['Terminal'] = df['location'].str.split('-').str[1]

    # assign supplier
    df['Supplier'] = 'Musket'

    # select columns
    df = df[['Supplier', 'Location', 'Terminal', 'Product', 'Price', 'Datetime', 'Date', 'Time']]
    
    return df

df_musket = process_musket_df(vendor_dfs)

#------------------------------------------------------------------------------------------------------------------
## offen
def process_offen_df(vendor_dfs):
    df = vendor_dfs['offen']

    # if terminal is 'Terms Net 10 Days via EFT or ACH' or 'Above prices are subject to midday changes and do not inculde any tax or freight' then drop it
    df = df[~df['Terminal'].isin(['Terms Net 10 Days via EFT or ACH', 'Above prices are subject to midday changes and do not inculde any tax or freight'])]

    # split into start and end date and time
    df['Datetime'] = pd.to_datetime(df['Effective'].str.split(' - ').str[0], format='%m/%d/%Y %I:%M %p')
    # df['End_Date'] = pd.to_datetime(df['Effective'].str.split(' - ').str[1], format='%m/%d/%Y %I:%M %p')

    # # for each row, duplicate, one with start and one with end dates
    # df = df.loc[df.index.repeat(2)].reset_index(drop=True)
    # df.loc[::2, 'Date'] = df['Start_Date']
    # df.loc[1::2, 'Date'] = df['End_Date']

    # date and time from datetime column
    df['Date'] = df['Datetime'].dt.strftime('%Y-%m-%d')
    df['Time'] = df['Datetime'].dt.strftime('%H:%M:%S')

    # assign supplier
    df['Supplier'] = 'Offen'

    # select columns
    df = df[['Supplier', 'Location', 'Terminal', 'Product', 'Price', 'Datetime', 'Date', 'Time']]

    return df

df_offen = process_offen_df(vendor_dfs)

#------------------------------------------------------------------------------------------------------------------
## rebel
def process_rebel_df(vendor_dfs):
    df = vendor_dfs['rebel']

    # if terminal is in any of these then drop it
    df = df[~df['Terminal'].isin([
        'Cyndi Maurycy|Wholesale Fuels Specialist', 
        'Office:  (702) 382-5866', 
        'Rebel Oil Company dba ROC', 
        'Cell: (725) 377-3598',
        '10650 W. Charleston Blvd., Suite 100Las Vegas, NV 89135',
        'Office:  (702) 382-5866',
        'UT'
    ])]

    # take first 3 characters of effective_datetime with / as delimiter
    df['Date_Int'] = df['Effective Datetime'].str.split(r'[-:]').str[0]

    # get date
    df['Date'] = pd.to_datetime(df['Date_Int'], format='mixed').dt.strftime('%Y-%m-%d')

    # time is always 00:00:00
    df['Time'] = '00:00:00'

    # create datetime column
    df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])

    # assign supplier
    df['Supplier'] = 'Rebel'

    # select columns
    df = df[['Supplier', 'Location', 'Terminal', 'Product', 'Price', 'Datetime', 'Date', 'Time']]

    return df

df_rebel = process_rebel_df(vendor_dfs)

#------------------------------------------------------------------------------------------------------------------
## shell
df_shell = vendor_dfs['shell']

# rename Product Name -> Product, Price -> Price
df_shell.rename(columns={'Product Name': 'Product'}, inplace=True)

# create Date and Time from effective_datetime
df_shell['Date'] = pd.to_datetime(df_shell['Effective Date'], format='mixed').dt.strftime('%Y-%m-%d')
df_shell['Time'] = pd.to_datetime(df_shell['Effective Date'], format='mixed').dt.strftime('%H:%M:%S')

# create datetime column
df_shell['Datetime'] = pd.to_datetime(df_shell['Date'] + ' ' + df_shell['Time'])

# assign supplier
df_shell['Supplier'] = 'Shell'

# seperate location column by - into location and terminal 
df_shell['Location'] = df_shell['Terminal Name'].str.split('-').str[0]
df_shell['Terminal'] = df_shell['Terminal Name'].str.split('-').str[1]

# select columns
df_shell = df_shell[['Supplier', 'Location', 'Terminal', 'Product', 'Price', 'Datetime', 'Date', 'Time']]

#------------------------------------------------------------------------------------------------------------------
## sinclair
def process_sinclair_df(vendor_dfs):
    df = vendor_dfs['sinclair']
    
    # make product and price column names uppercase
    df.rename(columns={'product': 'Product', 'price': 'Price'}, inplace=True)

    # create Date and Time from effective_datetime
    df['Date'] = pd.to_datetime(df['effective_datetime'], format='mixed').dt.strftime('%Y-%m-%d')
    df['Time'] = pd.to_datetime(df['effective_datetime'], format='mixed').dt.strftime('%H:%M:%S')

    # create datetime column
    df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])

    # seperate location column by - into location and terminal 
    df['Location'] = df['location'].str.split('-').str[0]
    df['Terminal'] = df['location'].str.split('-').str[1]

    # assign supplier
    df['Supplier'] = 'Sinclair'

    # select columns
    df = df[['Supplier', 'Location', 'Terminal', 'Product', 'Price', 'Datetime', 'Date', 'Time']]
    
    return df

df_sinclair = process_sinclair_df(vendor_dfs)

#------------------------------------------------------------------------------------------------------------------
## sunoco
def process_sunoco_df(vendor_dfs):
    df = vendor_dfs['sunoco']
    
    # make product and price column names uppercase
    df.rename(columns={'product': 'Product', 'price': 'Price'}, inplace=True)

    # create Date and Time from effective_datetime
    df['Date'] = pd.to_datetime(df['effective_datetime'], format='mixed').dt.strftime('%Y-%m-%d')
    df['Time'] = pd.to_datetime(df['effective_datetime'], format='mixed').dt.strftime('%H:%M:%S')

    # create datetime column
    df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])

    # seperate location column by - into location and terminal 
    df['Location'] = df['location'].str.split('-').str[0]
    df['Terminal'] = df['location'].str.split('-').str[1]

    # assign supplier
    df['Supplier'] = 'Sunoco'

    # select columns
    df = df[['Supplier', 'Location', 'Terminal', 'Product', 'Price', 'Datetime', 'Date', 'Time']]
    
    return df

df_sunoco = process_sunoco_df(vendor_dfs)

#------------------------------------------------------------------------------------------------------------------
## tartan
def process_tartan_df(vendor_dfs):
    df_tartan = vendor_dfs['tartan']

    # get date
    df_tartan['Date'] = pd.to_datetime(df_tartan['Effective Date'], format='mixed').dt.strftime('%Y-%m-%d')

    # time is always 00:00:00
    df_tartan['Time'] = '00:00:00'

    # create datetime column
    df_tartan['Datetime'] = pd.to_datetime(df_tartan['Date'] + ' ' + df_tartan['Time'])

    # assign supplier
    df_tartan['Supplier'] = 'Tartan'

    def cascade_fill_location_and_terminal(df):
        """
        Cascades location and terminal values down until new non-null values are found.
        
        Args:
            df (pd.DataFrame): DataFrame containing a 'Location' column
        
        Returns:
            pd.DataFrame: DataFrame with cascaded location and terminal values
        """
        
        # Add new Terminal column
        df['Terminal'] = None
        
        # Initialize variables
        current_location = None
        current_terminal = None
        
        # Main locations to track
        main_locations = {'Las Vegas', 'Salt Lake'}
        
        # Iterate through the rows
        for idx in range(len(df)):
            # Get the current location value
            loc = df.iloc[idx]['Location']
            
            # Process the row
            if pd.isna(loc):
                # If location is empty and we have current values, use them
                if current_location is not None:
                    df.at[idx, 'Location'] = current_location
                if current_terminal is not None:
                    df.at[idx, 'Terminal'] = current_terminal
            else:
                # Check if this is a main location or a terminal
                if loc in main_locations:
                    current_location = loc
                    current_terminal = None  # Reset terminal when new main location starts
                    df.at[idx, 'Terminal'] = None
                else:
                    # This is a terminal, keep the current_location and update terminal
                    if current_location is not None:
                        df.at[idx, 'Location'] = current_location
                    current_terminal = loc
                    df.at[idx, 'Terminal'] = current_terminal
                    
        return df

    # Apply the cascade fill
    df_tartan = cascade_fill_location_and_terminal(df_tartan)

    # if terminal is empty then set it to the location
    df_tartan['Terminal'] = df_tartan['Terminal'].fillna(df_tartan['Location'])

    # select columns
    df_tartan = df_tartan[['Supplier', 'Location', 'Terminal', 'Product', 'Price', 'Datetime', 'Date', 'Time']]
    
    return df_tartan

df_tartan = process_tartan_df(vendor_dfs)

#------------------------------------------------------------------------------------------------------------------
def process_valero_df(vendor_dfs):
    
    df = vendor_dfs['valero']
    
    # rename Product Name -> Product, Price -> Price
    df.rename(columns={'product': 'Product', 'price': 'Price'}, inplace=True)

    # create Date and Time from effective_datetime
    df['Date'] = pd.to_datetime(df['effective_datetime'], format='mixed').dt.strftime('%Y-%m-%d')
    df['Time'] = pd.to_datetime(df['effective_datetime'], format='mixed').dt.strftime('%H:%M:%S')

    # create datetime column
    df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])

    # assign supplier
    df['Supplier'] = 'Valero'

    # seperate location column by - into location and terminal 
    df['Terminal'] = df['terminal'].str.split(' ').str[0] + ' ' + df['terminal'].str.split(' ').str[1]
    df['Location'] = df['terminal'].str.split(' ').str[3] + ' ' + df['terminal'].str.split(' ').str[4] + ' ' + df['terminal'].str.split(' ').str[5]

    # select columns
    df = df[['Supplier', 'Location', 'Terminal', 'Product', 'Price', 'Datetime', 'Date', 'Time']]
    
    return df

df_valero = process_valero_df(vendor_dfs)

#------------------------------------------------------------------------------------------------------------------
df_chevron_tca = vendor_dfs['chevron-tca']

# create Date and Time from effective_datetime
df_chevron_tca['Date'] = pd.to_datetime(df_chevron_tca['Effective_Date'], format='mixed').dt.strftime('%Y-%m-%d')
df_chevron_tca['Time'] = pd.to_datetime(df_chevron_tca['Effective_Date'], format='mixed').dt.strftime('%H:%M:%S')

# create datetime column
df_chevron_tca['Datetime'] = pd.to_datetime(df_chevron_tca['Date'] + ' ' + df_chevron_tca['Time'])

#------------------------------------------------------------------------------------------------------------------
## opis
df_opis = vendor_dfs['opis']

# product = the text between "**OPIS NET TERMINAL" and "PRICES**" in the section column
df_opis['Product_Group'] = df_opis['section'].str.extract(r'\*\*OPIS NET TERMINAL(.*?)PRICES\*\*', expand=False).str.strip()

# if type is not u, b, or missing, then drop the row
df_opis = df_opis[
        (df_opis['type'].isin(['u', 'b'])) | 
        (df_opis['type'].isna()) |
        (df_opis['type'].isnull()) |
        (df_opis['type'] == '') |
        (df_opis['type'].str.strip() == '') |
        (pd.isna(df_opis['type']))
    ]

# if supplier contains "OPIS" then drop the row
df_opis = df_opis[~df_opis['supplier'].str.contains('OPIS')]
df_opis = df_opis[~df_opis['supplier'].str.contains('RENO, NV')]

# extract date from marketing_area column
# the date will always follow the format YYYY-MM-DD AFTER the state abbreviation
df_opis['Report_Date'] = df_opis['marketing_area'].str.extract(r'(\d{4}-\d{2}-\d{2})')

# year can be extracted from the report date where is it not null
df_opis['Year'] = df_opis['Report_Date'].str[:4]

# if supplier starts with "CONT" then extract (\d{2})/(\d{2}) from supplier and save it as agg_date
df_opis['Agg_Date'] = df_opis['supplier'].str.extract(r'(\d{2}/\d{2})')

# if CONT is in the supplier column, then remove the last 5 characters
df_opis['Supplier'] = df_opis['supplier'].apply(lambda x: 
        x[:-6] if isinstance(x, str) and 'CONT' in x 
        else x
    )

# product subgroups are housed in price1, price2, price3 columns and their respective changes are in move1, move2, move3 columns
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

# for each product group, get the product
df_opis['Product'] = df_opis['Product_Group'].map(product_group_mapping)

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

df_opis['Date'] = df_opis.apply(determine_date, axis=1)

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

# Example usage:
df_opis['Time'] = process_time(df_opis)

# clean up date and time columns
df_opis['Date'] = pd.to_datetime(df_opis['Date'], format='mixed').dt.strftime('%Y-%m-%d')
df_opis['Time'] = pd.to_datetime(df_opis['Time'], format='mixed').dt.strftime('%H:%M:%S')

# create datetime column
df_opis['Datetime'] = pd.to_datetime(df_opis['Date'] + ' ' + df_opis['Time'])

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

# Apply the function to create new columns
df_opis['location'] = df_opis.apply(extract_location, axis=1)

df_opis.rename(columns={'type': 'Brand', 'brand': 'location_code'}, inplace=True)

# select columns
df_opis = df_opis[['Supplier', 'location', "location_code", 'terminal', 'Product_Group', 'Product', 'price1', 'price2', 'price3', 'move1', 'move2', 'move3', 'Date', 'Time', 'Datetime', 'Brand', 'line_number', 'blob_name']]

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

# Example usage:
df_opis_expanded = assign_prices_optimized(df_opis)

# print distinct suppliers
print(df_opis['terminal'].unique())

#------------------------------------------------------------------------------------------------------------------
## marathon
df_marathon = vendor_dfs['marathon']

# rename Product Name -> Product, Price -> Price
df_marathon.rename(columns={'product': 'Product', 'price': 'Price', 'terminal': 'Terminal', 'tca': 'TCA'}, inplace=True)

# create Date and Time from effective_datetime
df_marathon['Date'] = pd.to_datetime(df_marathon['effective_datetime'], format='mixed').dt.strftime('%Y-%m-%d')
df_marathon['Time'] = pd.to_datetime(df_marathon['effective_datetime'], format='mixed').dt.strftime('%H:%M:%S')

# create datetime column
df_marathon['Datetime'] = pd.to_datetime(df_marathon['Date'] + ' ' + df_marathon['Time'])

# assign supplier
df_marathon['Supplier'] = 'Marathon'

# location is always empty
df_marathon['Location'] = ''

# select columns
df_marathon = df_marathon[['Supplier', 'Location', 'Terminal', 'Product', 'Price', 'Datetime', 'Date', 'Time']]

#------------------------------------------------------------------------------------------------------------------
# concat all the dataframes
df = pd.concat([df_bbenergy, df_bigwest, df_bradhall, df_chevron, df_kotaco, df_marathon, df_musket, df_offen, df_rebel, df_shell, df_sinclair, df_sunoco, df_tartan, df_valero])
df = df.drop_duplicates()

# order by supplier, location, terminal, product, and datetime
df = df.sort_values(by=['Supplier', 'Location', 'Terminal', 'Product', 'Datetime'])

# lag price by 1 day for each supplier, location, terminal, and product
df['Price_Yesterday'] = df.groupby(['Supplier', 'Location', 'Terminal', 'Product'])['Price'].shift(1)