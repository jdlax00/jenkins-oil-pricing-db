from utils.blob_operations import BlobStorageManager
import pandas as pd
from io import StringIO

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
        'Office:  (702) 382-5866',
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
# sort by marketing_area
df_opis = df_opis.sort_values(by=['marketing_area'])

# create Date and Time from effective_datetime
df_opis['Date'] = pd.to_datetime(df_opis['effective_datetime'], format='mixed').dt.strftime('%Y-%m-%d')
df_opis['Time'] = pd.to_datetime(df_opis['effective_datetime'], format='mixed').dt.strftime('%H:%M:%S')

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


