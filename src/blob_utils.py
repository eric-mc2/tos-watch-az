from azure.storage.blob import BlobServiceClient
import os
import logging
from azure.storage.blob import ContentSettings
import json
from src.log_utils import setup_logger
from collections import namedtuple
from pathlib import Path
from functools import lru_cache
from datetime import datetime, timezone

DEFAULT_CONTAINER = "documents"
logger = setup_logger(__name__, logging.INFO)
_client = None

def parse_blob_path(path: str, container: str = DEFAULT_CONTAINER):
    path = path.removeprefix(f"{container}/")
    blob_path = Path(path)
    Parts = namedtuple("BlobPath", ['stage','company','policy','timestamp'])
    return Parts(
        blob_path.parts[0],
        blob_path.parts[1],
        blob_path.parts[2],
        blob_path.stem)

def get_blob_service_client():
    global _client
    """Get blob service client from connection string environment variable"""
    connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    if not connection_string:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable not set")
    try:
        _client = BlobServiceClient.from_connection_string(connection_string)
    except Exception as e:
        raise ConnectionError(f"Failed to create BlobServiceClient:\n{e}") from e
    return _client

@lru_cache(5)
def ensure_container(output_container_name) -> None:
    client = get_blob_service_client()
    container_client = client.get_container_client(output_container_name)
    if container_client.exists():
        logger.debug(f"Output container {output_container_name} already exists.")
    else:
        container_client.create_container()
        logger.info(f"Created output container: {output_container_name}")

def check_blob(blob_name, container=DEFAULT_CONTAINER, touch=False) -> bool:
    client = get_blob_service_client()
    container_client = client.get_container_client(container)
    blob_client = container_client.get_blob_client(blob_name)
    if blob_client.exists() and touch:
        metadata = blob_client.get_blob_properties().metadata or {}
        metadata["touched"] = datetime.now(timezone.utc).isoformat()
        blob_client.set_blob_metadata(metadata)
    return blob_client.exists()

def list_blobs(container=DEFAULT_CONTAINER, strip_container=True) -> list[str]:
    client = get_blob_service_client()
    container_client = client.get_container_client(container)
    pages = container_client.list_blob_names()                      
    blobs = []
    for blob in pages:
        blobs.append(blob.removeprefix(f"{container}/"))
    return blobs


def list_blobs_nest(container=DEFAULT_CONTAINER, strip_container=True) -> dict:
    """Represent container as dictionary."""
    directory = {}
    for name in list_blobs(container, strip_container):
        namepath = Path(name)
        subdir = directory
        for i, part in enumerate(namepath.parts):
            if i == len(namepath.parts) - 1:
                leaf = subdir.setdefault(part, None)
            else:
                subdir = subdir.setdefault(part, {})
    return directory

def load_blob(name, container=DEFAULT_CONTAINER) -> str:
    logger.debug(f"Downloading blob: {container}/{name}")
    blob_service_client = get_blob_service_client()
    blob_client = blob_service_client.get_blob_client(container=container, blob=name)
    if blob_client.exists():
        data = blob_client.download_blob().readall()
        return data
    elif name.startswith(container):
        logger.warning(f"Blob {container}/{name} does not exist! Retrying with stripped container name.")
        name = name.removeprefix(f"{container}/")
        blob_client = blob_service_client.get_blob_client(container=container, blob=name)
        if blob_client.exists():
            data = blob_client.download_blob().readall()
            return data
        else:
            raise ValueError(f"Blob {container}/{name} does not exist!")
    else:
        raise ValueError(f"Blob {container}/{name} does not exist!")

def load_json_blob(name, container=DEFAULT_CONTAINER) -> dict:
    data = load_blob(name, container)
    try:
        json_data = json.loads(data.decode('utf-8'))
        return json_data
    except Exception as e:
        logger.error(f"Invalid json blob {name}:\n{e}")
        raise

def load_text_blob(name, container=DEFAULT_CONTAINER) -> dict:
    data = load_blob(name, container)
    try:
        txt = data.decode('utf-8')
    except Exception as e:
        logger.error(f"Error decoding text blob {name}:\n{e}")
        raise
    return txt

def upload_blob(data, blob_name, content_type, container=DEFAULT_CONTAINER) -> None:
    logger.debug(f"Uploading blob to {container}/{blob_name}")
    ensure_container(container)
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
    
def upload_text_blob(data, blob_name, container=DEFAULT_CONTAINER) -> None:
    data_bytes = data.encode('utf-8')
    content_type = 'text/plain; charset=utf-8'
    upload_blob(data_bytes, blob_name, content_type, container)

def upload_json_blob(data: str, blob_name, container=DEFAULT_CONTAINER) -> None:
    data_bytes = data.encode('utf-8')
    content_type = 'application/json; charset=utf-8'
    upload_blob(data_bytes, blob_name, content_type, container)

def upload_html_blob(cleaned_html, blob_name, container=DEFAULT_CONTAINER) -> None:
    html_bytes = cleaned_html.encode('utf-8')
    content_type = 'text/html; charset=utf-8'
    upload_blob(html_bytes, blob_name, content_type, container)
