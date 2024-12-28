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

# Define file categories - Add new categories here
FILE_CATEGORIES = {
    'abc': {
        'prefix': 'abc_',
        'tracker_key': 'last_copied_abc'
    },
    'edf': {
        'prefix': 'edf_',
        'tracker_key': 'last_copied_edf'
    }
    # Add new categories here like:
    # 'xyz': {
    #     'prefix': 'xyz_',
    #     'tracker_key': 'last_copied_xyz'
    # }
}

def get_category_blobs(blob_service_client, container_name, prefix: str, category_prefix: str) -> List[str]:
    """List all blobs for a specific category."""
    logger.info(f"Listing blobs for prefix '{prefix + category_prefix}'")
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blobs = container_client.list_blobs(name_starts_with=prefix + category_prefix)
        # Filter and sort files with extensions
        files = sorted([blob.name for blob in blobs if os.path.splitext(blob.name)[1] != ''])
        logger.info(f"Found {len(files)} files")
        logger.debug(f"Files: {files}")
        return files
    except Exception as e:
        logger.error(f"Error listing blobs: {str(e)}")
        raise

def process_category(
    category: str,
    config: dict,
    source_client: BlobServiceClient,
    target_client: BlobServiceClient,
    source_container: str,
    target_container: str,
    source_prefix: str,
    target_prefix: str,
    tracker_path: str
) -> Optional[str]:
    """Process a single category of files."""
    logger.info(f"Processing category: {category}")
    
    # Get files for this category
    files = get_category_blobs(source_client, source_container, source_prefix, config['prefix'])
    if not files:
        logger.info(f"No files found for category {category}")
        return None
        
    # Get last copied file for this category
    try:
        container_client = target_client.get_container_client(target_container)
        blob_client = container_client.get_blob_client(tracker_path)
        tracker_content = blob_client.download_blob().readall()
        tracker_data = json.loads(tracker_content)
        last_copied = tracker_data.get(config['tracker_key'])
        logger.info(f"Last copied file for {category}: {last_copied}")
    except Exception as e:
        if "BlobNotFound" in str(e):
            logger.warning(f"No {tracker_path} found. Starting from first file.")
            last_copied = None
        else:
            logger.error(f"Error reading tracker: {str(e)}")
            raise
    
    # Find next file to copy
    next_file = None
    if last_copied is None:
        next_file = files[0]
        logger.info(f"No previous file record found. Starting with: {next_file}")
    else:
        try:
            last_index = files.index(last_copied)
            if last_index + 1 < len(files):
                next_file = files[last_index + 1]
                logger.info(f"Next file to copy: {next_file}")
        except ValueError:
            next_file = files[0]
            logger.warning(f"Last copied file '{last_copied}' not found. Starting from beginning.")
    
    if next_file:
        # Copy the file
        target_path = target_prefix + os.path.basename(next_file)
        copy_file_server_side(
            source_account_name, source_account_key, source_container, next_file,
            target_account_name, target_account_key, target_container, target_path
        )
        logger.info(f"Successfully copied {category} file: {next_file}")
        return next_file
    
    return None

def update_last_copied_files(blob_service_client, container_name, copied_files: Dict[str, str], tracker_path):
    """Update the tracking blob with the last copied files for all categories."""
    logger.info(f"Updating tracker file '{tracker_path}' with copied files: {copied_files}")
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(tracker_path)
        
        # Prepare tracker content
        tracker_content = {
            "last_updated": datetime.utcnow().isoformat()
        }
        # Add entry for each category
        for category, config in FILE_CATEGORIES.items():
            tracker_content[config['tracker_key']] = copied_files.get(category)
        
        blob_client.upload_blob(json.dumps(tracker_content), overwrite=True)
        logger.info(f"Successfully updated {tracker_path}")
    except Exception as e:
        logger.error(f"Error updating {tracker_path}: {str(e)}")
        raise

def copy_file_server_side(source_account_name, source_account_key, source_container, source_blob_path,
                          target_account_name, target_account_key, target_container, target_blob_path):
    """Copy blob from source to target using server-side copy."""
    logger.info(f"Starting server-side copy from '{source_blob_path}' to '{target_blob_path}'")
    try:
        # Create source BlobServiceClient
        source_blob_service_client = BlobServiceClient(
            account_url=f"https://{source_account_name}.blob.core.windows.net",
            credential=source_account_key
        )
        
        # Get the source blob URL
        source_blob_client = source_blob_service_client.get_blob_client(source_container, source_blob_path)
        source_blob_url = source_blob_client.url
        logger.debug(f"Source blob URL: {source_blob_url}")

        # Create target BlobServiceClient
        target_blob_service_client = BlobServiceClient(
            account_url=f"https://{target_account_name}.blob.core.windows.net",
            credential=target_account_key
        )

        # Get target blob client
        target_blob_client = target_blob_service_client.get_blob_client(target_container, target_blob_path)

        # Initiate server-side copy
        copy_operation = target_blob_client.start_copy_from_url(source_blob_url)
        logger.info("Copy operation started")

        # Wait for the copy to complete
        properties = target_blob_client.get_blob_properties()
        while properties.copy.status == "pending":
            logger.info(f"Copy operation in progress: {properties.copy.status}")
            properties = target_blob_client.get_blob_properties()

        logger.info(f"Copy operation completed with status: {properties.copy.status}")
        
        if properties.copy.status == "success":
            logger.info(f"Successfully copied {source_blob_path} to {target_blob_path}")
        else:
            logger.error(f"Copy failed with status: {properties.copy.status}")
            if properties.copy.status == "failed":
                logger.error(f"Copy failed with error: {properties.copy.status_description}")

    except Exception as e:
        logger.error(f"Error during file copy: {str(e)}", exc_info=True)
        raise

# Example usage
if __name__ == "__main__":
    try:
        logger.info("Starting blob copy process")
        
        source_account_name = "your-source-account"
        source_account_key = "your-source-key"
        source_container = "source-container"
        source_folder = "source-folder/"
        
        target_account_name = "your-target-account"
        target_account_key = "your-target-key"
        target_container = "target-container"
        target_folder = "target-folder/"
        tracker_path = "stem/copy_tracker.json"

        logger.info(f"Source: {source_account_name}/{source_container}/{source_folder}")
        logger.info(f"Target: {target_account_name}/{target_container}/{target_folder}")
        
        source_client = BlobServiceClient(
            account_url=f"https://{source_account_name}.blob.core.windows.net",
            credential=source_account_key
        )
        
        target_client = BlobServiceClient(
            account_url=f"https://{target_account_name}.blob.core.windows.net",
            credential=target_account_key
        )

        copied_files = {}
        files_copied = False
        
        # Process each category sequentially
        for category, config in FILE_CATEGORIES.items():
            copied_file = process_category(
                category=category,
                config=config,
                source_client=source_client,
                target_client=target_client,
                source_container=source_container,
                target_container=target_container,
                source_prefix=source_folder,
                target_prefix=target_folder,
                tracker_path=tracker_path
            )
            
            if copied_file:
                copied_files[category] = copied_file
                files_copied = True
            else:
                # Get the last copied file for this category from tracker
                try:
                    container_client = target_client.get_container_client(target_container)
                    blob_client = container_client.get_blob_client(tracker_path)
                    tracker_content = blob_client.download_blob().readall()
                    tracker_data = json.loads(tracker_content)
                    copied_files[category] = tracker_data.get(config['tracker_key'])
                except Exception:
                    copied_files[category] = None
        
        # Update tracker if any files were copied
        if files_copied:
            update_last_copied_files(
                target_client,
                target_container,
                copied_files,
                tracker_path
            )
            logger.info("Blob copy process completed successfully")
        else:
            logger.info("No new files to copy in any category")
            
    except Exception as e:
        logger.error("Blob copy process failed", exc_info=True)
        raise
