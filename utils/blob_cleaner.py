import logging
from azure.storage.blob import BlobServiceClient, ContainerClient
import os
from dotenv import load_dotenv
from rich import print as rprint
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
import inquirer

load_dotenv()

def get_containers(connection_string: str) -> list:
    """Get list of available containers"""
    client = BlobServiceClient.from_connection_string(connection_string)
    return [container.name for container in client.list_containers()]

def get_subcontainers(connection_string: str, container: str) -> list:
    """Get list of available subcontainers (virtual directories)"""
    client = ContainerClient.from_connection_string(connection_string, container)
    # Get unique directory names from blob paths
    directories = set()
    for blob in client.list_blobs():
        parts = blob.name.split('/')
        if len(parts) > 1:
            directories.add(parts[0])
    return sorted(list(directories))

def clean_container(parent_container: str, sub_container: str = None, dry_run: bool = True) -> None:
    """
    Delete all blobs in a container.
    
    Args:
        parent_container (str): The parent container name
        sub_container (str): Optional sub-container name
        dry_run (bool): If True, only lists files that would be deleted without actually deleting
    """
    try:
        # Initialize blob service client
        connect_str = os.getenv('AZURE_WEB_JOBS_STORAGE')
        if not connect_str:
            raise ValueError("Missing AZURE_WEB_JOBS_STORAGE connection string")
            
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        
        # Get container client (without sub-container in path)
        container_client = blob_service_client.get_container_client(parent_container)
        
        # List blobs with prefix if sub-container is specified
        prefix = f"{sub_container}/" if sub_container else None
        blob_list = container_client.list_blobs(name_starts_with=prefix)
        
        # Count total blobs first
        blob_list = list(blob_list)  # Materialize the iterator
        total_blobs = len(blob_list)
        
        container_path = f"{parent_container}/{sub_container}" if sub_container else parent_container
        
        if total_blobs == 0:
            rprint(f"[#ffc300]No files found in {container_path}[/#ffc300]")
            return
        
        if dry_run:
            rprint(f"\n[#ffc300]DRY RUN - Found {total_blobs:,} files in {container_path}[/#ffc300]")
            rprint("[#ffc300]The following files would be deleted:[/#ffc300]")
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeRemainingColumn(),
            ) as progress:
                task = progress.add_task("Scanning files...", total=total_blobs)
                for blob in blob_list:
                    progress.update(task, advance=1, description=f"Scanning: {blob.name}")
                    
            rprint(f"\n[#ffc300]Total files that would be deleted: {total_blobs:,}[/#ffc300]")
            return
            
        # Delete all blobs
        deleted_count = 0
        error_count = 0
        
        rprint(f"\n[#FF6E6E]Starting deletion of {total_blobs:,} files from {container_path}...[/#FF6E6E]")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task("Deleting files...", total=total_blobs)
            
            for blob in blob_list:
                try:
                    container_client.delete_blob(blob.name)
                    deleted_count += 1
                    progress.update(task, advance=1, description=f"Deleting: {blob.name}")
                except Exception as e:
                    error_count += 1
                    rprint(f"[#FF6E6E]Error deleting {blob.name}: {str(e)}[/#FF6E6E]")
        
        # Final summary
        rprint(f"\n[#33cc99]Operation completed:[/#33cc99]")
        rprint(f"  • Total files processed: [#33cc99]{total_blobs:,}[/#33cc99]")
        rprint(f"  • Successfully deleted: [#33cc99]{deleted_count:,}[/#33cc99]")
        if error_count > 0:
            rprint(f"  • Failed to delete: [#FF6E6E]{error_count:,}[/#FF6E6E]")
        
    except Exception as e:
        rprint(f"[#FF6E6E]Critical error: {str(e)}[/#FF6E6E]")
        raise

if __name__ == "__main__":
    # Get connection string
    connect_str = os.getenv('AZURE_WEB_JOBS_STORAGE')
    if not connect_str:
        raise ValueError("Missing AZURE_WEB_JOBS_STORAGE connection string")

    # Get available containers
    containers = get_containers(connect_str)
    if not containers:
        rprint("[#FF6E6E]No containers found[/#FF6E6E]")
        exit(1)

    # Create container selection prompt
    questions = [
        inquirer.List('container',
                     message="Select container",
                     choices=containers),
    ]
    answers = inquirer.prompt(questions)
    container_name = answers['container']

    # Get subcontainers for selected container
    subcontainers = get_subcontainers(connect_str, container_name)
    if subcontainers:
        subcontainer_choices = ['None'] + subcontainers
        questions = [
            inquirer.List('subcontainer',
                         message="Select subcontainer",
                         choices=subcontainer_choices),
        ]
        answers = inquirer.prompt(questions)
        sub_container = answers['subcontainer']
        sub_container = None if sub_container == 'None' else sub_container
    else:
        sub_container = None
        rprint("[#ffc300]No subcontainers found[/#ffc300]")

    # First do a dry run
    clean_container(container_name, sub_container, dry_run=True)
    
    if inquirer.prompt([inquirer.Confirm('proceed', 
                                       message="Do you want to proceed with deletion?",
                                       default=False)])['proceed']:
        clean_container(container_name, sub_container, dry_run=False)
    else:
        rprint("[#ffc300]Operation cancelled[/#ffc300]") 