from azure.storage.blob import BlobServiceClient
import json
import logging
from datetime import datetime, timedelta
import os.path
from typing import Dict, List, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Define date formats
DATE_FORMAT = {
    'TRACKER': '%Y-%m-%d',      # Format for dates stored in tracker: 2025-01-26
    'FILENAME': '%Y%m%d',       # Format for dates in filenames: 20250126
    'LOGGING': '%Y-%m-%d',      # Format for dates in logs: 2025-01-26
}

# Define phases for data copying
PHASES = [
    {
        'id': 1,
        'start_date': '2025-01-01',
        'end_date': '2025-01-26'  # Current date
    },
    {
        'id': 2,
        'start_date': '2024-12-01',
        'end_date': '2024-12-31'
    },
    {
        'id': 3,
        'start_date': '2024-11-01',
        'end_date': '2024-11-30'
    }
]

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

def extract_date_from_filename(filename: str) -> str:
    """
    Extract date from filename and return in tracker format.
    Assumes filename contains date in FILENAME format.
    """
    try:
        # Extract the date part from filename
        date_str = next(part for part in os.path.basename(filename).split('_') 
                       if len(part) == 8 and part.isdigit())
        # Convert from filename format to tracker format
        date_obj = datetime.strptime(date_str, DATE_FORMAT['FILENAME'])
        return date_obj.strftime(DATE_FORMAT['TRACKER'])
    except (ValueError, StopIteration):
        logger.warning(f"Could not extract date from filename: {filename}")
        return None

def get_data_files(blob_service_client, container_name: str, prefix: str, category_prefix: str, 
                   data_extension: str, processing_date: str) -> List[str]:
    """
    Get list of data files for a specific date.
    processing_date should be in TRACKER format.
    """
    try:
        container_client = blob_service_client.get_container_client(container_name)
        full_prefix = prefix + category_prefix
        logger.info(f"Listing data files with prefix: {full_prefix}")
        
        # Convert processing_date from tracker format to filename format
        date_obj = datetime.strptime(processing_date, DATE_FORMAT['TRACKER'])
        date_for_file = date_obj.strftime(DATE_FORMAT['FILENAME'])
        
        # List blobs and filter for specific date
        blobs = container_client.list_blobs(name_starts_with=full_prefix)
        data_files = []
        
        for blob in blobs:
            if not blob.name.endswith(data_extension):
                continue
                
            # Check if file matches the processing date
            try:
                filename = os.path.basename(blob.name)
                date_str = next(part for part in filename.split('_') 
                              if len(part) == 8 and part.isdigit())
                if date_str == date_for_file:
                    data_files.append(blob.name)
            except (ValueError, StopIteration):
                continue
        
        data_files.sort()
        logger.info(f"Found {len(data_files)} data files for date {processing_date}")
        return data_files
        
    except Exception as e:
        logger.error(f"Error listing data files: {str(e)}")
        raise

def get_last_processed_date(tracker_content: dict) -> str:
    """
    Get the last processed date by checking all category files.
    Returns the most recent date in TRACKER format.
    """
    last_dates = []
    for category in FILE_CATEGORIES:
        last_file = tracker_content.get(FILE_CATEGORIES[category]['tracker_key'])
        if last_file:
            date = extract_date_from_filename(last_file)
            if date:
                last_dates.append(date)
    
    return max(last_dates) if last_dates else None

def get_next_processing_date(blob_service_client, container_name: str, tracker_path: str) -> str:
    """
    Determine the next date to process.
    If no tracker exists, start with first phase's start date.
    If tracker exists, get next date after last processed date.
    Returns the next date to process in TRACKER format, or None if all phases complete.
    """
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(tracker_path)
        
        try:
            tracker_content = json.loads(blob_client.download_blob().readall())
            last_processed_date = get_last_processed_date(tracker_content)
            
            if not last_processed_date:
                # If no last processed date, start with first phase
                logger.info(f"No last processed date found. Starting with phase 1: {PHASES[0]['start_date']}")
                return PHASES[0]['start_date']
                
            # Find current phase based on last processed date
            current_phase = None
            next_phase = None
            
            for i, phase in enumerate(PHASES):
                if phase['start_date'] <= last_processed_date <= phase['end_date']:
                    current_phase = phase
                    next_phase = PHASES[i + 1] if i + 1 < len(PHASES) else None
                    break
            
            if not current_phase:
                logger.error(f"Last processed date {last_processed_date} does not belong to any phase. "
                           f"This might indicate data corruption. Starting from phase 1.")
                return PHASES[0]['start_date']
            
            # Calculate next date
            last_date = datetime.strptime(last_processed_date, DATE_FORMAT['TRACKER'])
            next_date = last_date + timedelta(days=1)
            next_date_str = next_date.strftime(DATE_FORMAT['TRACKER'])
                
            # If next_date is within current phase, continue with it
            if next_date_str <= current_phase['end_date']:
                logger.info(f"Processing next date {next_date_str} in phase {current_phase['id']}")
                return next_date_str
                
            # If we've completed current phase and there's a next phase, 
            # move to next phase's start date
            if next_phase:
                logger.info(f"Phase {current_phase['id']} completed on {last_processed_date}. "
                          f"Moving to phase {next_phase['id']} starting from {next_phase['start_date']}")
                return next_phase['start_date']
            
            # If we're here, we've completed all phases
            logger.info(f"All phases completed! Last processed date was {last_processed_date}")
            return None
            
        except Exception as e:
            if "BlobNotFound" in str(e):
                logger.info(f"No tracker found. Starting with phase 1: {PHASES[0]['start_date']}")
                return PHASES[0]['start_date']
            raise
            
    except Exception as e:
        logger.error(f"Error determining next processing date: {str(e)}")
        raise

def update_tracker(blob_service_client, container_name: str, tracker_path: str,
                  category: str, copied_file: str):
    """Update tracker with processed file information."""
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(tracker_path)
        
        # Try to get existing tracker content
        try:
            tracker_content = json.loads(blob_client.download_blob().readall())
        except Exception:
            tracker_content = {}
        
        # Update tracker
        tracker_content[FILE_CATEGORIES[category]['tracker_key']] = copied_file
        tracker_content['last_updated'] = datetime.utcnow().isoformat()
        
        # Upload updated tracker
        blob_client.upload_blob(json.dumps(tracker_content), overwrite=True)
        
        # Get current date and phase for logging
        current_date = extract_date_from_filename(copied_file)
        current_phase = next((phase for phase in PHASES 
                            if phase['start_date'] <= current_date <= phase['end_date']), None)
        phase_id = current_phase['id'] if current_phase else 'unknown'
        
        logger.info(f"Updated tracker for {category} with file: {copied_file} "
                   f"(Phase {phase_id})")
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
                    source_folder: str, target_folder: str, tracker_path: str, 
                    processing_date: str) -> bool:
    """Process files for a single category for specific date."""
    # Find current phase for logging
    current_phase = next((phase for phase in PHASES 
                         if phase['start_date'] <= processing_date <= phase['end_date']), None)
    phase_id = current_phase['id'] if current_phase else 'unknown'
    
    logger.info(f"Processing category: {category} for date: {processing_date} (Phase {phase_id})")
    config = FILE_CATEGORIES[category]
    
    # Get list of data files for this date
    data_files = get_data_files(
        source_client,
        source_container,
        source_folder,
        config['prefix'],
        config['data_extension'],
        processing_date
    )
    
    if not data_files:
        logger.info(f"No files found for category {category} on date {processing_date}")
        return False
    
    # Process each file for this date
    files_copied = False
    for file in data_files:
        logger.info(f"Copying file: {file}")
        if copy_files(
            source_client,
            target_client,
            source_container,
            target_container,
            file,
            target_folder
        ):
            update_tracker(
                source_client,
                source_container,
                tracker_path,
                category,
                file
            )
            files_copied = True
    
    return files_copied

def main():
    """Main function to orchestrate the file copying process."""
    try:
        # Get configuration from environment variables
        connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        if not connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable not set")
        
        source_container = os.getenv('SOURCE_CONTAINER')
        target_container = os.getenv('TARGET_CONTAINER')
        source_folder = os.getenv('SOURCE_FOLDER', '')
        target_folder = os.getenv('TARGET_FOLDER', '')
        tracker_path = os.getenv('TRACKER_PATH', 'copy_tracker.json')
        
        if not all([source_container, target_container]):
            raise ValueError("Required environment variables not set")
        
        # Create blob service clients
        source_client = BlobServiceClient.from_connection_string(connection_string)
        target_client = source_client  # Using same client for source and target
        
        # Get the next date to process
        processing_date = get_next_processing_date(source_client, source_container, tracker_path)
        
        if not processing_date:
            logger.info("All phases have been completed. No more dates to process.")
            return
            
        # Find current phase for logging
        current_phase = next((phase for phase in PHASES 
                            if phase['start_date'] <= processing_date <= phase['end_date']), None)
        phase_id = current_phase['id'] if current_phase else 'unknown'
        logger.info(f"Processing data for date: {processing_date} (Phase {phase_id})")
        
        # Process each category for the current date
        files_copied = False
        for category in FILE_CATEGORIES:
            if process_category(
                category,
                source_client, target_client,
                source_container, target_container,
                source_folder, target_folder,
                tracker_path,
                processing_date
            ):
                files_copied = True
        
        if files_copied:
            logger.info(f"Successfully copied files for date {processing_date}")
        else:
            logger.info(f"No files to copy for date {processing_date}")
            # Update tracker even if no files were copied to move to next date
            update_tracker(
                source_client,
                source_container,
                tracker_path,
                "no_files",
                ""
            )
            
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main()
