import azure.functions as func
from extractors.shell_extractor import process_shell_email
from staging import shell_staging
from canonical import can_pipeline

app = func.FunctionApp()

# Email triggers for raw data extraction
@app.function_name(name="ShellExtractor")
@app.outlook_message_trigger(
    name="email",
    connection="Office365Connection",
    from_address="Shell-MarketHub-US-Fuels@shell.com",
    to_address="fuelprices@jenkinsoil.com"
)
def shell_extractor(email: func.EmailMessage) -> None:
    process_shell_email(email)

# Blob trigger for data cleaning
@app.function_name(name="ShellDataCleaner")
@app.blob_trigger(path="raw-data-container/shell_{name}", connection="AzureWebJobsStorage")
def shell_cleaning_trigger(blob: func.InputStream) -> None:
    shell_staging.clean_shell_data(blob)

# # Timer trigger for data consolidation
# @app.function_name(name="MasterConsolidator")
# @app.timer_trigger(schedule="0 0 * * * *")  # Run hourly
# def consolidation_trigger(timer: func.TimerRequest) -> None:
#     master_consolidator.consolidate_data()