import azure.functions as func
import json
from utils.graph_email_processor_v2 import GraphEmailProcessor
import logging

app = func.FunctionApp()

# Shell Email Trigger
@app.function_name(name="ShellExtractor")
@app.event_grid_trigger(arg_name="event")
def shell_extractor(event: func.EventGridEvent) -> None:
    logging.info('Python EventGrid trigger function processed an event')
    
    # Get the email data from the event
    result = json.loads(event.get_json())
    
    if result.get('from', {}).get('emailAddress', {}).get('address') == 'shell-markethub-us-fuels@shell.com':
        logging.info(f"Processing Shell email")
        config = {
            'vendor': 'Shell',
            'process_attachments': True,
        }
        processor = GraphEmailProcessor()
        processor.process_historical_message(result, config)

# BBEnergy Email Trigger
@app.function_name(name="BBEnergyExtractor")
@app.event_grid_trigger(arg_name="event")
def bbenergy_extractor(event: func.EventGridEvent) -> None:
    result = json.loads(event.get_json())
    
    if (result.get('from', {}).get('emailAddress', {}).get('address') == 'petromail@dtnenergy.com' and
        'BBE1' in result.get('subject', '')):
        logging.info(f"Processing BBEnergy email")
        config = {
            'vendor': 'BBEnergy',
            'process_attachments': False,
        }
        processor = GraphEmailProcessor()
        processor.process_historical_message(result, config)

# Offen Email Trigger
@app.function_name(name="OffenExtractor")
@app.event_grid_trigger(arg_name="event")
def offen_extractor(event: func.EventGridEvent) -> None:
    result = json.loads(event.get_json())
    
    if (result.get('from', {}).get('emailAddress', {}).get('address') == 'Pricing@offenpetro.com'):
        logging.info(f"Processing Offen email")
        config = {
            'vendor': 'Offen',
            'process_attachments': True,
        }
        processor = GraphEmailProcessor()
        processor.process_historical_message(result, config)

# BBEnergy Data Parser
@app.function_name(name="BBEnergyDataParser")
@app.blob_trigger(arg_name="myblob", 
                  path="jenkins-pricing-historical/bbenergy", 
                  connection="AzureWebJobsStorage")
def bbenergy_data_parser(event: func.EventGridEvent) -> None:
    result = json.loads(event.get_json())
    logging.info(f"Processing BBEnergy data")
    