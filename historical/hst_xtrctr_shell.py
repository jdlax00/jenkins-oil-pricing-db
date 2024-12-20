import datetime
from utils.graph_email_processor_v2 import GraphEmailProcessor

def get_historical_emails(start_date: datetime.datetime, end_date: datetime.datetime) -> None:
    """Fetch and process historical Shell emails within the specified date range."""
        
    config = {
        'vendor': 'Shell',
        'start_date': start_date,
        'end_date': end_date,
        'sender_address': 'shell-markethub-us-fuels@shell.com',
        'process_attachments': True,
    }
    
    processor = GraphEmailProcessor()
    processor.get_historical_emails(config)

if __name__ == "__main__":
    start_date = datetime.datetime(2010, 1, 1)
    end_date = datetime.datetime.now()
    
    try:
        get_historical_emails(start_date, end_date)
        print("Historical extraction completed successfully")
    except Exception as e:
        print(f"Error during historical extraction: {str(e)}") 