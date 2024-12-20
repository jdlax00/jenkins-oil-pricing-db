import logging
import datetime
import mimetypes
from azure.core.exceptions import AzureError
import azure.functions as func
from utils.blob_operations import BlobStorageManager

def process_shell_email(email: func.EmailMessage) -> None:
    """
    Process emails from Shell and save attachments to blob storage.
    
    Args:
        email (func.EmailMessage): The incoming email message from Office 365
    """
    try:
        logging.info('Processing new Shell email message')
        
        # Validate email sender for additional security
        sender = email.from_address.lower()
        if sender != 'shell-markethub-us-fuels@shell.com':
            logging.error(f"Unauthorized sender: {sender}")
            return
            
        # Initialize blob manager
        blob_manager = BlobStorageManager("jenkins-pricing-shell")
            
        # Process attachments
        attachments = email.attachments
        if not attachments:
            logging.warning("No attachments found in email")
            return
            
        processed_count = 0
        for attachment in attachments:
            try:
                # Generate unique blob name
                timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                safe_filename = ''.join(c for c in attachment.name if c.isalnum() or c in '._-')
                blob_name = f"shell_pricing_{timestamp}_{safe_filename}"
                
                # Determine content type
                content_type = mimetypes.guess_type(attachment.name)[0] or 'application/octet-stream'
                logging.info(f"Processing attachment: {attachment.name} (type: {content_type})")
                
                # Upload with metadata
                metadata = {
                    'source': 'shell_email',
                    'original_filename': attachment.name,
                    'email_subject': email.subject,
                    'email_received': email.received_time.isoformat() if email.received_time else ''
                }
                
                blob_manager.upload_blob(
                    blob_name,
                    attachment.content,
                    content_type=content_type,
                    metadata=metadata
                )
                
                processed_count += 1
                logging.info(f"Successfully uploaded attachment: {blob_name}")
                
            except Exception as e:
                logging.error(f"Error processing attachment {attachment.name}: {str(e)}")
                continue
        
        logging.info(f"Successfully processed {processed_count} out of {len(attachments)} attachments")
        
    except Exception as e:
        logging.error(f"Critical error in Shell email processor: {str(e)}")
        raise