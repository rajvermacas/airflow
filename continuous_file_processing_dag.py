from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.microsoft.azure.operators.adls import ADLSDeleteOperator, ADLSListOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import logging
import os
from typing import Any, Dict, Optional
import json
from airflow.hooks.base import BaseHook
import tempfile

# Define your Azure Data Lake Storage connection details
ADLS_CONN_ID = 'azure_data_lake_default'
STORAGE_ACCOUNT = 'your-storage-account'
SOURCE_CONTAINER = 'source-container'
DEST_CONTAINER = 'destination-container'
SOURCE_PATH = 'input/'
DEST_PATH = 'processed/'

# Azure authentication details
# These should be configured in Airflow's connection
"""
Connection Name: azure_data_lake_default
Connection Type: Azure Data Lake
Login (client_id): Your Azure AD Application (Client) ID
Password (client_secret): Your Azure AD Application Client Secret
Extra: {
    "tenant_id": "Your Azure AD Tenant ID",
    "account_name": "Your ADLS Gen2 Storage Account Name"
}
"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

class AzCopyOperator(BashOperator):
    """
    Custom operator to copy files between Azure Data Lake Storage containers using azcopy.
    This operator requires azcopy to be installed on the Airflow worker nodes.
    """
    
    template_fields = ('source_path', 'dest_path')
    
    def __init__(
        self,
        source_path: str,
        dest_path: str,
        azure_conn_id: Optional[str] = None,
        source_sas_token: Optional[str] = None,
        dest_sas_token: Optional[str] = None,
        azcopy_flags: Optional[str] = None,
        **kwargs: Any
    ) -> None:
        """
        Initialize the AzCopyOperator.
        
        :param source_path: Source URL
        :param dest_path: Destination URL
        :param azure_conn_id: Connection ID for Azure credentials
        :param source_sas_token: Optional SAS token for source if not using service principal
        :param dest_sas_token: Optional SAS token for destination if not using service principal
        :param azcopy_flags: Additional azcopy command flags
        :param kwargs: Additional arguments passed to BashOperator
        """
        self.source_path = source_path
        self.dest_path = dest_path
        self.azure_conn_id = azure_conn_id
        self.source_sas_token = source_sas_token
        self.dest_sas_token = dest_sas_token
        self.azcopy_flags = azcopy_flags or ''
        
        # Get Azure credentials if connection ID is provided
        if azure_conn_id:
            conn = BaseHook.get_connection(azure_conn_id)
            if conn.extra_dejson:
                # Create temp file for service principal auth
                fd, auth_file = tempfile.mkstemp()
                try:
                    with os.fdopen(fd, 'w') as f:
                        auth_config = {
                            "tenantId": conn.extra_dejson.get("tenant_id"),
                            "clientId": conn.login,
                            "clientSecret": conn.password,
                            "activeDirectoryEndpointUrl": "https://login.microsoftonline.com",
                            "resourceManagerEndpointUrl": "https://management.azure.com/",
                            "activeDirectoryGraphResourceId": "https://graph.windows.net/",
                            "sqlManagementEndpointUrl": "https://management.core.windows.net:8443/",
                            "galleryEndpointUrl": "https://gallery.azure.com/",
                            "managementEndpointUrl": "https://management.core.windows.net/"
                        }
                        json.dump(auth_config, f)
                    
                    # Login using service principal
                    login_command = f"azcopy login --service-principal --application-id {conn.login} " \
                                  f"--tenant-id {conn.extra_dejson.get('tenant_id')} " \
                                  f"--certificate-path {auth_file}"
                    
                    # Execute login command
                    bash_command = f"{login_command} && "
                finally:
                    os.unlink(auth_file)
            else:
                raise ValueError(f"Azure connection {azure_conn_id} does not contain required service principal credentials")
        else:
            bash_command = ""

        # Construct the azcopy copy command
        source_url = source_path
        if source_sas_token and '?' not in source_path:
            source_url = f"{source_path}?{source_sas_token}"
            
        dest_url = dest_path
        if dest_sas_token and '?' not in dest_path:
            dest_url = f"{dest_path}?{dest_sas_token}"
            
        bash_command += f"azcopy copy '{source_url}' '{dest_url}' {self.azcopy_flags}"
        
        super().__init__(
            bash_command=bash_command,
            **kwargs
        )

def process_file(**context):
    """
    Function to process the moved file
    Add your processing logic here
    """
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['AZURE_STORAGE_CONNECTION_STRING'])
    task_instance = context['task_instance']
    latest_file = task_instance.xcom_pull(task_ids="get_latest_file")
    
    # Read the blob content
    blob_client = blob_service_client.get_blob_client(DEST_CONTAINER, DEST_PATH + latest_file.split('/')[-1])
    blob_content = blob_client.download_blob().content_as_text()
    
    # Add your processing logic here
    processed_content = blob_content  # Replace with actual processing
    
    logging.info(f"Processed file: {latest_file}")
    return latest_file

def get_latest_file(**context):
    """
    Get the latest file from the source path
    """
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['AZURE_STORAGE_CONNECTION_STRING'])
    container_client = blob_service_client.get_container_client(SOURCE_CONTAINER)
    blobs = container_client.list_blobs(name_starts_with=SOURCE_PATH)
    if not blobs:
        logging.info("No files found in source path")
        return None
    
    latest_blob = max(blobs, key=lambda x: x.last_modified)
    logging.info(f"Latest file: {latest_blob.name}")
    return latest_blob.name

def construct_sas_url(**context):
    """
    Construct SAS URLs for source and destination
    """
    task_instance = context['task_instance']
    latest_file = task_instance.xcom_pull(task_ids="get_latest_file")
    source_url = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net/{SOURCE_CONTAINER}/{SOURCE_PATH}{latest_file}"
    dest_url = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net/{DEST_CONTAINER}/{DEST_PATH}{latest_file}"
    return {"source_url": source_url, "dest_url": dest_url}

def trigger_next_task(**context):
    """
    Trigger the next task
    """
    # Add your logic to trigger the next task
    pass

with DAG(
    'continuous_file_processing',
    default_args=default_args,
    description='Continuous file processing DAG for Azure Blob Storage',
    schedule_interval=None,
    catchup=False
) as dag:
    
    # Task to list files in source path
    list_files = ADLSListOperator(
        task_id='list_files',
        azure_data_lake_conn_id=ADLS_CONN_ID,
        path=SOURCE_PATH
    )

    # Task to get the latest file
    get_latest = PythonOperator(
        task_id='get_latest_file',
        python_callable=get_latest_file,
        provide_context=True
    )

    # Task to prepare URLs
    prepare_urls = PythonOperator(
        task_id='prepare_urls',
        python_callable=construct_sas_url,
        provide_context=True
    )

    # Task to copy file using AzCopy
    copy_file = AzCopyOperator(
        task_id='copy_file',
        source_path='{{ task_instance.xcom_pull(task_ids="prepare_urls")["source_url"] }}',
        dest_path='{{ task_instance.xcom_pull(task_ids="prepare_urls")["dest_url"] }}',
        azure_conn_id='azure_service_principal',
        azcopy_flags='--recursive=true --overwrite=true'
    )

    # Task to process the copied file
    process_copied_file = PythonOperator(
        task_id='process_file',
        python_callable=process_file,
        provide_context=True
    )

    # Delete the source file after successful copy
    delete_source = ADLSDeleteOperator(
        task_id='delete_source',
        azure_data_lake_conn_id=ADLS_CONN_ID,
        path=SOURCE_PATH + '{{ task_instance.xcom_pull(task_ids="get_latest_file").split("/")[-1] }}'
    )

    # Task to trigger the next run
    trigger_next = PythonOperator(
        task_id='trigger_next',
        python_callable=trigger_next_task,
        provide_context=True
    )

    # Define the task dependencies
    list_files >> get_latest >> prepare_urls >> copy_file >> process_copied_file >> delete_source >> trigger_next
