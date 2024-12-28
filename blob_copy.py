from azure.storage.blob import BlobServiceClient
import json

def get_blob_list(blob_service_client, container_name, prefix=""):
    """List all blobs in the container with the given prefix."""
    container_client = blob_service_client.get_container_client(container_name)
    blobs = container_client.list_blobs(name_starts_with=prefix)
    return sorted([blob.name for blob in blobs])

def get_last_copied_file(blob_service_client, container_name, tracker_path):
    """Get the last copied file from the tracking blob.
    Args:
        blob_service_client: Azure blob service client
        container_name: Name of the container
        tracker_path: Full path to the tracker file within the container (e.g. 'stem/copy_tracker.json')
    """
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(tracker_path)
        tracker_content = blob_client.download_blob().readall()
        return json.loads(tracker_content)["last_copied_file"]
    except Exception as e:
        if "BlobNotFound" in str(e):
            print(f"No {tracker_path} found. Will create it after first file copy.")
        else:
            print(f"Error reading {tracker_path}: {e}")
        return None

def update_last_copied_file(blob_service_client, container_name, last_file, tracker_path):
    """Update the tracking blob with the last copied file.
    Args:
        blob_service_client: Azure blob service client
        container_name: Name of the container
        last_file: Path of the last copied file
        tracker_path: Full path to the tracker file within the container (e.g. 'stem/copy_tracker.json')
    """
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(tracker_path)
        tracker_content = json.dumps({"last_copied_file": last_file})
        blob_client.upload_blob(tracker_content, overwrite=True)
        print(f"Successfully updated {tracker_path} with last copied file: {last_file}")
    except Exception as e:
        print(f"Error updating {tracker_path}: {e}")
        raise

def get_next_blob_to_copy(blob_service_client, container_name, prefix="", tracker_path="copy_tracker.json"):
    """Get the next blob that needs to be copied."""
    all_blobs = get_blob_list(blob_service_client, container_name, prefix)
    if not all_blobs:
        return None
        
    last_copied = get_last_copied_file(blob_service_client, container_name, tracker_path)
    
    if last_copied is None:
        return all_blobs[0]
    
    try:
        last_index = all_blobs.index(last_copied)
        if last_index + 1 < len(all_blobs):
            return all_blobs[last_index + 1]
    except ValueError:
        return all_blobs[0]
    
    return None

def copy_file_server_side(source_account_name, source_account_key, source_container, source_blob_path,
                          target_account_name, target_account_key, target_container, target_blob_path):
    try:
        # Create source BlobServiceClient
        source_blob_service_client = BlobServiceClient(
            account_url=f"https://{source_account_name}.blob.core.windows.net",
            credential=source_account_key
        )
        
        # Get the source blob URL
        source_blob_client = source_blob_service_client.get_blob_client(source_container, source_blob_path)
        source_blob_url = source_blob_client.url

        # Create target BlobServiceClient
        target_blob_service_client = BlobServiceClient(
            account_url=f"https://{target_account_name}.blob.core.windows.net",
            credential=target_account_key
        )

        # Get target blob client
        target_blob_client = target_blob_service_client.get_blob_client(target_container, target_blob_path)

        # Initiate server-side copy
        copy_operation = target_blob_client.start_copy_from_url(source_blob_url)

        # Wait for the copy to complete
        properties = target_blob_client.get_blob_properties()
        while properties.copy.status == "pending":
            print(f"Copy operation in progress: {properties.copy.status}")
            properties = target_blob_client.get_blob_properties()

        print(f"Copy operation completed: {properties.copy.status}")
        print(f"File copied successfully from {source_blob_path} to {target_blob_path}")

    except Exception as e:
        print(f"Error during file copy: {e}")

# Example usage
if __name__ == "__main__":
    source_account_name = "your-source-account"
    source_account_key = "your-source-key"
    source_container = "source-container"
    source_folder = "source-folder/"  # Specify your source folder prefix here

    target_account_name = "your-target-account"
    target_account_key = "your-target-key"
    target_container = "target-container"
    target_folder = "target-folder/"  # Specify your target folder prefix here
    tracker_path = "stem/copy_tracker.json"  # Specify the path to your tracker file

    # Create source blob service client
    source_blob_service_client = BlobServiceClient(
        account_url=f"https://{source_account_name}.blob.core.windows.net",
        credential=source_account_key
    )

    # Get the next blob to copy
    source_blob_path = get_next_blob_to_copy(
        source_blob_service_client, 
        source_container, 
        source_folder,
        tracker_path
    )
    
    if source_blob_path:
        # Construct target blob path
        target_blob_path = target_folder + source_blob_path.split('/')[-1]
        
        # Copy the file
        copy_file_server_side(
            source_account_name, source_account_key, source_container, source_blob_path,
            target_account_name, target_account_key, target_container, target_blob_path
        )
        
        # Update the tracker
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
    else:
        print("No new files to copy")
