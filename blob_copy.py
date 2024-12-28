from azure.storage.blob import BlobServiceClient
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def get_blob_list(blob_service_client, container_name, prefix=""):
    """List all blobs in the container with the given prefix."""
    logger.info(f"Listing blobs in container '{container_name}' with prefix '{prefix}'")
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blobs = container_client.list_blobs(name_starts_with=prefix)
        blob_list = sorted([blob.name for blob in blobs])
        logger.info(f"Found {len(blob_list)} blobs")
        logger.debug(f"Blob list: {blob_list}")
        return blob_list
    except Exception as e:
        logger.error(f"Error listing blobs: {str(e)}")
        raise

def get_last_copied_file(blob_service_client, container_name, tracker_path):
    """Get the last copied file from the tracking blob."""
    logger.info(f"Reading last copied file from '{tracker_path}'")
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(tracker_path)
        tracker_content = blob_client.download_blob().readall()
        last_file = json.loads(tracker_content)["last_copied_file"]
        logger.info(f"Last copied file was: {last_file}")
        return last_file
    except Exception as e:
        if "BlobNotFound" in str(e):
            logger.warning(f"No {tracker_path} found. Will create it after first file copy.")
        else:
            logger.error(f"Error reading {tracker_path}: {str(e)}")
        return None

def update_last_copied_file(blob_service_client, container_name, last_file, tracker_path):
    """Update the tracking blob with the last copied file."""
    logger.info(f"Updating tracker file '{tracker_path}' with last copied file: {last_file}")
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(tracker_path)
        tracker_content = json.dumps({
            "last_copied_file": last_file,
            "last_updated": datetime.utcnow().isoformat()
        })
        blob_client.upload_blob(tracker_content, overwrite=True)
        logger.info(f"Successfully updated {tracker_path}")
    except Exception as e:
        logger.error(f"Error updating {tracker_path}: {str(e)}")
        raise

def get_next_blob_to_copy(blob_service_client, container_name, prefix="", tracker_path="copy_tracker.json"):
    """Get the next blob that needs to be copied."""
    logger.info(f"Finding next blob to copy in container '{container_name}'")
    all_blobs = get_blob_list(blob_service_client, container_name, prefix)
    if not all_blobs:
        logger.warning("No blobs found in the source container")
        return None
        
    last_copied = get_last_copied_file(blob_service_client, container_name, tracker_path)
    
    if last_copied is None:
        next_blob = all_blobs[0]
        logger.info(f"No previous copy record found. Starting with first blob: {next_blob}")
        return next_blob
    
    try:
        last_index = all_blobs.index(last_copied)
        if last_index + 1 < len(all_blobs):
            next_blob = all_blobs[last_index + 1]
            logger.info(f"Next blob to copy: {next_blob}")
            return next_blob
        else:
            logger.info("All blobs have been copied")
            return None
    except ValueError:
        logger.warning(f"Last copied file '{last_copied}' not found in current blob list. Starting from beginning.")
        return all_blobs[0]

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
        
        source_blob_service_client = BlobServiceClient(
            account_url=f"https://{source_account_name}.blob.core.windows.net",
            credential=source_account_key
        )

        source_blob_path = get_next_blob_to_copy(
            source_blob_service_client, 
            source_container, 
            source_folder,
            tracker_path
        )
        
        if source_blob_path:
            target_blob_path = target_folder + source_blob_path.split('/')[-1]
            
            copy_file_server_side(
                source_account_name, source_account_key, source_container, source_blob_path,
                target_account_name, target_account_key, target_container, target_blob_path
            )
            
            target_blob_service_client = BlobServiceClient(
                account_url=f"https://{target_account_name}.blob.core.windows.net",
                credential=target_account_key
            )
            update_last_copied_file(
                target_blob_service_client, 
                target_container, 
                source_blob_path,
                tracker_path
            )
            logger.info("Blob copy process completed successfully")
        else:
            logger.info("No new files to copy")
            
    except Exception as e:
        logger.error("Blob copy process failed", exc_info=True)
        raise
