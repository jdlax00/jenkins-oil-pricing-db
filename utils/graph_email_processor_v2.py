import logging
import datetime
import os
from msal import ConfidentialClientApplication
import requests
from utils.blob_operations import BlobStorageManager
import base64
import mimetypes
from rich import print as rprint
import time
import psutil
import re
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn

class GraphEmailProcessor:
    def __init__(self):
        """Initialize the Graph API email processor using environment variables"""
        self.client_id = os.getenv('AZURE_CLIENT_ID')
        self.client_secret = os.getenv('AZURE_CLIENT_SECRET')
        self.tenant_id = os.getenv('AZURE_TENANT_ID')
        
        if not all([self.client_id, self.client_secret, self.tenant_id]):
            raise ValueError("Missing required credentials in environment variables")
            
        self.scopes = ['https://graph.microsoft.com/.default']
        self.graph_url = 'https://graph.microsoft.com/v1.0'
        self.authority_url = f'https://login.microsoftonline.com/{self.tenant_id}'
        self.process = psutil.Process(os.getpid())

    def _log_operation(self, message: str, vendor: str = None, is_error: bool = False, memory: bool = True) -> None:
        """Helper method for consistent logging format"""
        mem = f"[bright_white]{self.process.memory_info().rss / (1024**2):,.0f}MB[/bright_white]" if memory else ""
        vendor_str = f" [reverse]{vendor}[/reverse] " if vendor else " " * 7
        
        message = re.sub(r'(\d+(?:\.\d+)?)', r'[#33cc99]\1[/#33cc99]', message)
        
        if is_error:
            rprint(f" {'GraphEmail':14} {mem:8} {vendor_str}  ⎹  [#FF6E6E]{message}[/#FF6E6E]")
        else:
            rprint(f" {'GraphEmail':14} {mem:8} {vendor_str}  ⎹  {message}")

    def _validate_config(self, config: dict) -> None:
        """Validate required configuration parameters"""
        required_fields = ['vendor', 'start_date', 'end_date', 'sender_address']
        missing_fields = [field for field in required_fields if field not in config]
        
        if missing_fields:
            raise ValueError(f"Missing required configuration fields: {', '.join(missing_fields)}")
        
    def get_access_token(self):
        """Get Microsoft Graph API access token"""
        app = ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=self.authority_url
        )
        
        result = app.acquire_token_silent(self.scopes, account=None)
        if not result:
            result = app.acquire_token_for_client(scopes=self.scopes)
            
        if 'access_token' not in result:
            raise Exception(f"Error getting access token: {result.get('error_description', 'Unknown error')}")
        return result['access_token']
    
    def _build_initial_request(self, config: dict) -> str:
        """Build the initial request URL"""
        self._log_operation("Building initial request", config['vendor'])
        return f"{self.graph_url}/users/fuelpricesarchive@jenkinsoil.com/messages"
    
    def _build_filter_query(self, config: dict) -> str:
        """Build the filter query string"""
        formatted_start = config['start_date'].strftime('%Y-%m-%dT%H:%M:%SZ')
        formatted_end = config['end_date'].strftime('%Y-%m-%dT%H:%M:%SZ')
        
        filter_query = f"from/emailAddress/address eq '{config['sender_address']}' and "
        if config.get('subject_filter'):
            filter_query += f"contains(subject, '{config['subject_filter']}') and "
        filter_query += f"receivedDateTime ge {formatted_start} and receivedDateTime le {formatted_end}"
        
        return filter_query

    def get_historical_emails(self, config: dict) -> None:
        """Generic function to fetch and process historical emails."""
        try:
            start_time = time.time()
            self._validate_config(config)
            
            # Extract config values
            vendor = config['vendor']
            start_date = config['start_date']
            end_date = config['end_date']
            sender_address = config['sender_address']
            subject_filter = config.get('subject_filter', '')
            
            # Get total count first
            token = self.get_access_token()
            total_count = self._get_total_email_count(token, config)
            self._log_operation(f"Found total of {total_count} emails matching criteria", vendor)
            
            if total_count == 0:
                self._log_operation("No emails found matching criteria", vendor)
                return
            
            self._log_operation(f"Initializing email fetch for {vendor}", vendor)
            self._log_operation(f"Date range: {start_date.date()} to {end_date.date()}", vendor)
            self._log_operation(f"Sender: {sender_address}", vendor)
            if subject_filter:
                self._log_operation(f"Subject filter: {subject_filter}", vendor)

            processed_count = 0
            error_count = 0
            retry_count = 0
            max_retries = 3
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TextColumn("({task.completed}/{task.total})"),
                TimeRemainingColumn(),
            ) as progress:
                task = progress.add_task("Processing emails...", total=total_count)
                
                while processed_count < total_count:
                    try:
                        # Refresh token periodically
                        if processed_count % 900 == 0:
                            token = self.get_access_token()
                            headers = {
                                'Authorization': f'Bearer {token}',
                                'Content-Type': 'application/json'
                            }
                        
                        # Calculate skip for pagination
                        skip = processed_count
                        
                        # Query parameters with skip
                        params = {
                            '$filter': self._build_filter_query(config),
                            '$select': 'subject,receivedDateTime,body,hasAttachments',
                            '$expand': 'attachments',
                            '$top': 100,  # Reduced batch size
                            '$skip': skip
                        }
                        
                        response = requests.get(
                            f"{self.graph_url}/users/fuelpricesarchive@jenkinsoil.com/messages",
                            headers=headers,
                            params=params
                        )
                        
                        if response.status_code == 429:  # Too Many Requests
                            retry_after = int(response.headers.get('Retry-After', 60))
                            self._log_operation(f"Rate limited. Waiting {retry_after} seconds...", vendor)
                            time.sleep(retry_after)
                            continue

                        if response.status_code == 400:
                            self._log_operation(f"Error fetching emails: {response.text}", vendor, True)
                            break

                        if response.status_code == 403:
                            self._log_operation(f"Access denied: {response.text}", vendor, True)
                            break

                        if response.status_code == 404:
                            self._log_operation(f"Resource not found: {response.text}", vendor, True)
                            break
                            
                        if response.status_code != 200:
                            raise Exception(f"Error fetching emails: {response.text}")
                        
                        messages = response.json().get('value', [])
                        if not messages:
                            break
                        
                        for message in messages:
                            self.process_historical_message(message, config)
                            processed_count += 1
                            progress.update(task, advance=1, description=f"Processing: {message.get('subject', 'No subject')[:50]}")
                        
                        # Add delay between batches
                        time.sleep(1)
                        retry_count = 0  # Reset retry count on successful batch
                        
                    except Exception as e:
                        retry_count += 1
                        if retry_count >= max_retries:
                            raise Exception(f"Maximum retries reached: {str(e)}")
                        
                        self._log_operation(f"Error in batch. Retry {retry_count}/{max_retries}: {str(e)}", vendor, True)
                        time.sleep(retry_count * 30)  # Exponential backoff
                        continue
                    
        except Exception as e:
            self._log_operation(f"Batch processing failed: {str(e)}", vendor, True)
            raise
            
        # Final summary
        end_time = time.time()
        duration = end_time - start_time
        
        rprint(f"\n[#33cc99]Operation completed:[/#33cc99]")
        rprint(f"  • Total emails found: [#33cc99]{total_count:,}[/#33cc99]")
        rprint(f"  • Successfully processed: [#33cc99]{processed_count:,}[/#33cc99]")
        if error_count > 0:
            rprint(f"  • Failed to process: [#FF6E6E]{error_count:,}[/#FF6E6E]")
        rprint(f"  • Time elapsed: [#33cc99]{duration:.2f}[/#33cc99] seconds")

    def _get_total_email_count(self, token: str, config: dict) -> int:
        """Get total count of emails matching the filter criteria"""
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        params = {
            '$filter': self._build_filter_query(config),
            '$count': 'true'
        }
        
        url = f"{self.graph_url}/users/fuelpricesarchive@jenkinsoil.com/messages"
        response = requests.get(
            url, 
            headers={**headers, 'ConsistencyLevel': 'eventual'}, # Required for $count
            params=params
        )
        
        if response.status_code != 200:
            raise Exception(f"API request failed: {response.text}")
        
        data = response.json()
        return data.get('@odata.count', 0)
    
    def process_historical_message(self, message: dict, config: dict) -> None:
        """Process message and save attachments to blob storage."""
        try:
            blob_manager = BlobStorageManager(f"jenkins-pricing-historical/{config['vendor'].lower()}")
            self._log_operation(f"↘ Processing message: {message['subject']}", config['vendor'])

            if config.get('process_attachments', False):
                self._process_attachments(message, blob_manager, config)
            else:
                self._process_email_body(message, blob_manager, config)
                    
        except Exception as e:
            self._log_operation(f"Error processing historical message: {str(e)}", config['vendor'], True)
            raise

    def _process_attachments(self, message: dict, blob_manager: BlobStorageManager, config: dict) -> None:
        """Process email attachments"""
        
        for attachment in message.get('attachments', []):
            try:
                # Generate unique blob name
                received_date = message['receivedDateTime']
                received_date = datetime.datetime.strptime(received_date, '%Y-%m-%dT%H:%M:%SZ')
                timestamp = received_date.strftime("%Y%m%d_%H%M%S")
                # safe_filename = ''.join(c for c in attachment['name'] if c.isalnum() or c in '._-')
                # blob_name = f"{config['vendor'].lower()}_pricing_{timestamp}_{safe_filename}"
                random_id = str(hash(attachment['name'] + timestamp))[-6:]  # Last 6 digits of hash
                blob_name = f"{config['vendor'].lower()}_pricing_{timestamp}_{random_id}"
                self._log_operation(f"  Processing: {blob_name}", config['vendor'])
                
                # Determine content type
                content_type = mimetypes.guess_type(attachment['name'])[0] or 'application/octet-stream'
                
                # Get attachment content
                content_bytes = base64.b64decode(attachment['contentBytes'])
                
                # Encode metadata values as ASCII with replacement characters
                metadata = {
                    'source': f"{config['vendor'].lower()}_historical_email",
                    'original_filename': attachment['name'].encode('ascii', 'replace').decode('ascii'),
                    'email_subject': message['subject'].encode('ascii', 'replace').decode('ascii'),
                    'email_received': message['receivedDateTime']
                }
                
                blob_manager.upload_blob(
                    blob_name,
                    content_bytes,
                    content_type=content_type,
                    metadata=metadata
                )
                
                self._log_operation(f"  Successfully uploaded attachment: {attachment['name']}", config['vendor'])
            
            except Exception as e:
                self._log_operation(f"Error processing attachment {attachment['name']}: {str(e)}", config['vendor'], True)
                continue

    def _process_email_body(self, message: dict, blob_manager: BlobStorageManager, config: dict) -> None:
        """Process email body"""
        received_date = message['receivedDateTime']
        received_date = datetime.datetime.strptime(received_date, '%Y-%m-%dT%H:%M:%SZ')
        timestamp = received_date.strftime("%Y%m%d_%H%M%S")
        safe_subject = ''.join(c for c in message['subject'] if c.isalnum() or c in '._-')[:50]
        blob_name = f"{config['vendor'].lower()}_pricing_{timestamp}_{safe_subject}.txt"
        
        body_content = message['body'].get('content', '')
        
        metadata = {
            'source': f"{config['vendor'].lower()}_historical_email",
            'email_subject': message['subject'],
            'email_received': message['receivedDateTime']
        }
        
        blob_manager.upload_blob(
            blob_name,
            body_content.encode('utf-8'),
            content_type='text/plain',
            metadata=metadata
        )
        
        self._log_operation(f"  Processed email body: {safe_subject}", config['vendor'])