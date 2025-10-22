from azure.storage.blob import BlobServiceClient
import os
import logging

def get_blob_service_client():
    """Get blob service client from connection string environment variable"""
    connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    if not connection_string:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable not set")
    try:
        client = BlobServiceClient.from_connection_string(connection_string)
    except Exception as e:
        raise ConnectionError(f"Failed to create BlobServiceClient: {e}")
    return client

def ensure_container(client, output_container_name):
    # Ensure output container exists
    try:
        container_client = client.get_container_client(output_container_name)
        container_client.create_container()
        logging.info(f"Created output container: {output_container_name}")
    except Exception as e:
        # Container might already exist, which is fine
        logging.info(f"Output container {output_container_name} already exists or creation failed: {e}")
    