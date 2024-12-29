from azure.storage.blob import BlobServiceClient
import json
import logging
from datetime import datetime
import os.path
from typing import Dict, List, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Define file categories
FILE_CATEGORIES = {
    'abc': {
        'prefix': 'abc_',
        'tracker_key': 'last_copied_abc',
        'data_extension': '.txt'
    },
    'edf': {
        'prefix': 'edf_',
        'tracker_key': 'last_copied_edf',
        'data_extension': '.txt'
    }
}

def get_data_files(blob_service_client, container_name: str, prefix: str, category_prefix: str, 
                   data_extension: str) -> List[str]:
    """Get list of data files (excluding CTL files) for a category."""
    try:
        container_client = blob_service_client.get_container_client(container_name)
        full_prefix = prefix + category_prefix
        logger.info(f"Listing data files with prefix: {full_prefix}")
        
        # List blobs and filter for data files
        blobs = container_client.list_blobs(name_starts_with=full_prefix)
        data_files = sorted([
            blob.name for blob in blobs 
            if blob.name.endswith(data_extension)
        ])
        
        logger.info(f"Found {len(data_files)} data files")
        logger.debug(f"Data files: {data_files}")
        return data_files
    except Exception as e:
        logger.error(f"Error listing data files: {str(e)}")
        raise

def get_last_copied_file(blob_service_client, container_name: str, tracker_path: str, 
                        category: str) -> Optional[str]:
    """Get last copied file for a specific category."""
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(tracker_path)
        tracker_content = blob_client.download_blob().readall()
        tracker_data = json.loads(tracker_content)
        last_file = tracker_data.get(FILE_CATEGORIES[category]['tracker_key'])
        logger.info(f"Last copied file for {category}: {last_file}")
        return last_file
    except Exception as e:
        if "BlobNotFound" in str(e):
            logger.warning(f"No {tracker_path} found. Will start from beginning.")
        else:
            logger.error(f"Error reading tracker: {str(e)}")
        return None

def update_tracker(blob_service_client, container_name: str, tracker_path: str,
                  category: str, copied_file: str):
    """Update tracker for a specific category."""
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(tracker_path)
        
        # Try to get existing tracker content
        try:
            tracker_content = json.loads(blob_client.download_blob().readall())
        except Exception:
            tracker_content = {}
        
        # Update tracker with new file and timestamp
        tracker_content[FILE_CATEGORIES[category]['tracker_key']] = copied_file
        tracker_content['last_updated'] = datetime.utcnow().isoformat()
        
        # Upload updated tracker
        blob_client.upload_blob(json.dumps(tracker_content), overwrite=True)
        logger.info(f"Updated tracker for {category} with file: {copied_file}")
    except Exception as e:
        logger.error(f"Error updating tracker: {str(e)}")
        raise

def copy_files(source_client: BlobServiceClient, target_client: BlobServiceClient,
               source_container: str, target_container: str,
               source_data_path: str, target_folder: str) -> bool:
    """Copy both data and CTL files."""
    try:
        # Generate file paths
        source_ctl_path = os.path.splitext(source_data_path)[0] + '.ctl'
        target_data_path = target_folder + os.path.basename(source_data_path)
        target_ctl_path = target_folder + os.path.basename(source_ctl_path)
        
        # Check if CTL file exists
        source_container_client = source_client.get_container_client(source_container)
        if not source_container_client.get_blob_client(source_ctl_path).exists():
            logger.error(f"CTL file not found: {source_ctl_path}")
            return False
        
        # Copy data file
        logger.info(f"Copying data file: {source_data_path}")
        source_data_client = source_client.get_blob_client(source_container, source_data_path)
        target_data_client = target_client.get_blob_client(target_container, target_data_path)
        data_copy = target_data_client.start_copy_from_url(source_data_client.url)
        
        # Copy CTL file
        logger.info(f"Copying CTL file: {source_ctl_path}")
        source_ctl_client = source_client.get_blob_client(source_container, source_ctl_path)
        target_ctl_client = target_client.get_blob_client(target_container, target_ctl_path)
        ctl_copy = target_ctl_client.start_copy_from_url(source_ctl_client.url)
        
        # Wait for copies to complete
        data_prop = target_data_client.get_blob_properties()
        ctl_prop = target_ctl_client.get_blob_properties()
        
        while data_prop.copy.status == 'pending' or ctl_prop.copy.status == 'pending':
            data_prop = target_data_client.get_blob_properties()
            ctl_prop = target_ctl_client.get_blob_properties()
        
        if data_prop.copy.status == 'success' and ctl_prop.copy.status == 'success':
            logger.info("Successfully copied both data and CTL files")
            return True
        else:
            logger.error(f"Copy failed. Data status: {data_prop.copy.status}, CTL status: {ctl_prop.copy.status}")
            return False
            
    except Exception as e:
        logger.error(f"Error during file copy: {str(e)}")
        return False

def process_category(category: str, source_client: BlobServiceClient, target_client: BlobServiceClient,
                    source_container: str, target_container: str,
                    source_folder: str, target_folder: str, tracker_path: str) -> bool:
    """Process files for a single category."""
    logger.info(f"Processing category: {category}")
    config = FILE_CATEGORIES[category]
    
    # Get list of data files
    data_files = get_data_files(
        source_client, 
        source_container,
        source_folder,
        config['prefix'],
        config['data_extension']
    )
    
    if not data_files:
        logger.info(f"No files found for category {category}")
        return False
    
    # Get last copied file
    last_copied = get_last_copied_file(
        target_client,
        target_container,
        tracker_path,
        category
    )
    
    # Find next file to copy
    next_file = None
    if last_copied is None:
        next_file = data_files[0]
    else:
        try:
            last_index = data_files.index(last_copied)
            if last_index + 1 < len(data_files):
                next_file = data_files[last_index + 1]
        except ValueError:
            logger.warning(f"Last copied file not found in source. Starting from beginning.")
            next_file = data_files[0]
    
    if next_file is None:
        logger.info(f"No new files to copy for category {category}")
        return False
    
    # Copy files and update tracker
    if copy_files(
        source_client, target_client,
        source_container, target_container,
        next_file, target_folder
    ):
        update_tracker(
            target_client,
            target_container,
            tracker_path,
            category,
            next_file
        )
        return True
    
    return False

def main():
    try:
        logger.info("Starting blob copy process")
        
        # Configuration
        source_account_name = "your-source-account"
        source_account_key = "your-source-key"
        source_container = "source-container"
        source_folder = "source-folder/"
        
        target_account_name = "your-target-account"
        target_account_key = "your-target-key"
        target_container = "target-container"
        target_folder = "target-folder/"
        tracker_path = "stem/copy_tracker.json"
        
        # Create clients
        source_client = BlobServiceClient(
            account_url=f"https://{source_account_name}.blob.core.windows.net",
            credential=source_account_key
        )
        
        target_client = BlobServiceClient(
            account_url=f"https://{target_account_name}.blob.core.windows.net",
            credential=target_account_key
        )
        
        # Process each category
        files_copied = False
        for category in FILE_CATEGORIES:
            if process_category(
                category,
                source_client, target_client,
                source_container, target_container,
                source_folder, target_folder,
                tracker_path
            ):
                files_copied = True
        
        if files_copied:
            logger.info("Successfully copied files from one or more categories")
        else:
            logger.info("No new files to copy in any category")
            
    except Exception as e:
        logger.error("Blob copy process failed", exc_info=True)
        raise

if __name__ == "__main__":
    """
    Flow:
        main
            process_category
                get_data_files
                get_last_copied_file
                copy_files
                update_tracker
    """
    main()
