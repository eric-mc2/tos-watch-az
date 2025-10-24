from azure.storage.blob import BlobServiceClient
import os
import logging
from azure.storage.blob import ContentSettings
import json
from src.log_utils import setup_logger
from collections import namedtuple
from pathlib import Path

logger = setup_logger(logging.INFO)

def parse_blob_path(path: str):
    blob_path = Path(path)
    Parts = namedtuple("BlobPath", ['stage','company','policy','timestamp'])
    return Parts(
        blob_path.parts[0],
        blob_path.parts[1],
        blob_path.parts[2],
        blob_path.stem)

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
    client = None
    try:
        client = get_blob_service_client()
        container_client = client.get_container_client(output_container_name)
        if container_client.exists():
            logger.debug(f"Output container {output_container_name} already exists.")
        else:
            container_client.create_container()
            logger.info(f"Created output container: {output_container_name}")
    except Exception as e:
        logger.error(f"Output container {output_container_name} creation failed: {e}")
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
        logger.error(f"Existence check for {output_container_name}/{blob_name} failed.")
        raise e
    finally:
        if client:
            client.close()

def load_blob(container, name):
    try:
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(container=container, blob=name)
        data = blob_client.download_blob().readall()
        logger.info(f"Loaded blob storage: {container}/{name}")
    except Exception as e:
        logger.error(f"Failed to load blob {container}/{name}: {e}")
        raise e
    finally:
        if blob_service_client:
            blob_service_client.close()
    return data

def load_json_blob(container, name):
    data = load_blob(container, name)
    try:
        json_data = json.loads(data.decode('utf-8'))
    except Exception as e:
        logger.error(f"Invalid json blob {name}: {e}")
        raise e
    return json_data

def upload_blob(data, container, blob_name, content_type):
    try:
        blob_service_client = get_blob_service_client()
        # Upload to blob storage with explicit UTF-8 encoding
        blob_client = blob_service_client.get_blob_client(
            container=container, 
            blob=blob_name
        )
        # Ensure we upload as UTF-8 bytes
        blob_client.upload_blob(
            data, 
            overwrite=True,
            content_settings=ContentSettings(
                content_type=content_type,
                cache_control='max-age=2592000'
            )
        )
    except Exception as e:
        logger.error(f"Error uploading blob: {e}")
        raise e
    finally:
        if blob_service_client:
            blob_service_client.close()

def upload_json_blob(data, output_container_name, blob_name):
    data_bytes = data.encode('utf-8')
    content_type = 'application/json; charset=utf-8'
    upload_blob(data_bytes, output_container_name, blob_name, content_type)

def upload_html_blob(cleaned_html, output_container_name, blob_name):
    html_bytes = cleaned_html.encode('utf-8')
    content_type = 'text/html; charset=utf-8'
    upload_blob(html_bytes, output_container_name, blob_name, content_type)
