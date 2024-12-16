from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

def list_adls_containers(storage_account_name, filesystem_name, path):
    try:
        # Initialize the credentials using Default Azure Credential (managed identity)
        credential = DefaultAzureCredential()
        
        # Print the account URL for debugging
        account_url = f"https://{storage_account_name}.dfs.core.windows.net"
        print(f"Attempting to connect to: {account_url}")
        
        # Create the DataLakeServiceClient
        service_client = DataLakeServiceClient(
            account_url=account_url,
            credential=credential
        )
        
        # Verify connection by listing file systems
        try:
            # List at least one file system to verify connection
            next(service_client.list_file_systems(), None)
            print(f"Successfully connected to storage account")
        except Exception as e:
            print(f"Failed to connect to storage account: {str(e)}")
            raise
        
        # Get the filesystem client
        print(f"Attempting to access filesystem: {filesystem_name}")
        filesystem_client = service_client.get_file_system_client(filesystem_name)
        
        # Try to check if filesystem exists
        try:
            filesystem_exists = filesystem_client.exists()
            print(f"Filesystem exists: {filesystem_exists}")
            if not filesystem_exists:
                raise Exception(f"Filesystem {filesystem_name} does not exist")
        except Exception as e:
            print(f"Error checking filesystem: {str(e)}")
            raise
        
        # List all paths in the specified directory
        print(f"Attempting to list paths in: {path}")
        paths = filesystem_client.get_paths(path=path)
        
        # Print and collect the paths
        container_list = []
        for path in paths:
            container_list.append(path.name)
            print(f"Found path: {path.name}")
            
        return container_list
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        print("Please verify:")
        print(f"1. Storage account '{storage_account_name}' exists and is accessible")
        print(f"2. Managed identity has 'Storage Blob Data Reader' role assigned")
        print(f"3. Network connectivity to Azure is working")
        print(f"4. Filesystem '{filesystem_name}' exists")
        raise

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5)
}

# Create the DAG
dag = DAG(
    'list_adls_containers',
    default_args=default_args,
    description='List containers in ADLS using managed identity',
    schedule_interval=None,
    start_date=datetime(2024, 12, 13),
    catchup=False
)

# Create the task
list_containers_task = PythonOperator(
    task_id='list_containers_task',
    python_callable=list_adls_containers,
    op_kwargs={
        'storage_account_name': 'mystorageaccount',    # From: mystorageaccount.dfs.core.windows.net
        'filesystem_name': 'mycontainer',              # From: abfss://mycontainer@
        'path': 'folder1/subfolder'                    # From: /folder1/subfolder
    },
    dag=dag
)

# Example ABFSS path mapping:
# abfss://mycontainer@mystorageaccount.dfs.core.windows.net/folder1/subfolder
#        ^^^^^^^^^^ ^^^^^^^^^^^^^^^^                    ^^^^^^^^^^^^^^^^^^^^^^^^
#     filesystem    storage_account                           path