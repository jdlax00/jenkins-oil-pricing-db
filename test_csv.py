from utils.blob_operations import BlobStorageManager
import pandas as pd
from io import StringIO

vendor = "bigwest"

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

df