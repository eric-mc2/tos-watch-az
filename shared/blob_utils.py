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

def ensure_container(output_container_name):
    try:
        client = get_blob_service_client()
        container_client = client.get_container_client(output_container_name)
        if container_client.exists():
            logging.debug(f"Output container {output_container_name} already exists.")
        else:
            container_client.create_container()
            logging.info(f"Created output container: {output_container_name}")
    except Exception as e:
        logging.error(f"Output container {output_container_name} creation failed: {e}")
        raise e
    finally:
        if client:
            client.close()

def check_blob(output_container_name, blob_name):
    try:
        client = get_blob_service_client()
        container_client = client.get_container_client(output_container_name)
        blob_client = container_client.get_blob_client(blob_name)
        return blob_client.exists()
    except Exception as e:
        logging.error(f"Existence check for {output_container_name}/{blob_name} failed.")
        raise e
    finally:
        if client:
            client.close()