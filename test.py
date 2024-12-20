from dotenv import load_dotenv
import os
import requests
from msal import ConfidentialClientApplication

# Load environment variables from .env file and print current directory
print(f"Current working directory: {os.getcwd()}")
load_dotenv(verbose=True)  # Enable verbose loading

class GraphEmailReader:
    def __init__(self):
        """
        Initialize the Graph API email reader using environment variables
        """
        # Get environment variables with explicit debug output
        self.client_id = os.getenv('AZURE_CLIENT_ID')
        self.client_secret = os.getenv('AZURE_CLIENT_SECRET')
        self.tenant_id = os.getenv('AZURE_TENANT_ID')
        
        # Debug print all environment variables (sanitized)
        print("\nEnvironment Variables Debug:")
        print(f"AZURE_CLIENT_ID: {self.client_id[:5]}... (length: {len(str(self.client_id)) if self.client_id else 0})")
        print(f"AZURE_CLIENT_SECRET: {'[PRESENT]' if self.client_secret else '[MISSING]'}")
        print(f"AZURE_TENANT_ID: {self.tenant_id} (length: {len(str(self.tenant_id)) if self.tenant_id else 0})")
        
        # Validate tenant_id specifically
        if not self.tenant_id or self.tenant_id.lower() == 'none':
            raise ValueError(
                f"Invalid or missing AZURE_TENANT_ID. Current value: '{self.tenant_id}'. "
                f"Please check your .env file at: {os.path.abspath('.env')}"
            )
        
        if not all([self.client_id, self.client_secret]):
            raise ValueError(
                "Missing required credentials. Please ensure AZURE_CLIENT_ID and "
                "AZURE_CLIENT_SECRET are set in your .env file."
            )
            
        self.scopes = ['https://graph.microsoft.com/.default']
        self.graph_url = 'https://graph.microsoft.com/v1.0'
        
        # Print authority URL for debugging
        self.authority_url = f'https://login.microsoftonline.com/{self.tenant_id}'
        print(f"\nAuthority URL: {self.authority_url}")
        
    def get_access_token(self):
        """Get Microsoft Graph API access token"""
        try:
            app = ConfidentialClientApplication(
                client_id=self.client_id,
                client_credential=self.client_secret,
                authority=self.authority_url
            )
            
            result = app.acquire_token_silent(self.scopes, account=None)
            if not result:
                result = app.acquire_token_for_client(scopes=self.scopes)
                
            if 'access_token' in result:
                return result['access_token']
            else:
                raise Exception(f"Error getting access token: {result.get('error_description', 'Unknown error')}")
        except Exception as e:
            print(f"\nError details:")
            print(f"Authority URL: {self.authority_url}")
            print(f"Tenant ID: {self.tenant_id}")
            raise
            
    def read_emails(self, email_address, top=10):
        """Read emails from specified email address"""
        token = self.get_access_token()
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        endpoint = f"{self.graph_url}/users/{email_address}/messages"
        params = {
            '$top': top,
            '$select': 'subject,sender,receivedDateTime,bodyPreview',
            '$orderby': 'receivedDateTime DESC'
        }
        
        response = requests.get(endpoint, headers=headers, params=params)
        
        if response.status_code == 200:
            emails = response.json().get('value', [])
            return [{
                'subject': email.get('subject', ''),
                'sender': email.get('sender', {}).get('emailAddress', {}).get('address', ''),
                'received_time': email.get('receivedDateTime', ''),
                'preview': email.get('bodyPreview', '')
            } for email in emails]
        else:
            raise Exception(f"Error reading emails: {response.text}")
        
email_reader = GraphEmailReader()

# Read latest 10 emails
emails = email_reader.read_emails('fuelpricesarchive@jenkinsoil.com', top=10)

# Print out the emails
for email in emails:
    print(f"\nSubject: {email['subject']}")
    print(f"From: {email['sender']}")
    print(f"Received: {email['received_time']}")
    print(f"Preview: {email['preview']}")