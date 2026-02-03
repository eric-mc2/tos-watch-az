from azure.storage.blob import BlobServiceClient, BlobClient
import os
import logging
from azure.storage.blob import ContentSettings
import json
from src.utils.log_utils import setup_logger
from collections import namedtuple
from pathlib import Path
from functools import lru_cache
from datetime import datetime, timezone
import atexit

logger = setup_logger(__name__, logging.INFO)

DEFAULT_CONTAINER = "documents"
DEFAULT_CONNECTION = "AzureWebJobsStorage"
_client = None
_connection_key = DEFAULT_CONNECTION

def set_connection_key(key: str = DEFAULT_CONNECTION):
    global _connection_key
    global _client
    _connection_key = key
    if _client:
        _client.close()
        _client = None

def get_connection_key():
    global _connection_key
    return _connection_key

def parse_blob_path(path: str, container: str = DEFAULT_CONTAINER):
    path = path.removeprefix(f"{container}/")
    blob_path = Path(path)
    if len(blob_path.parts) == 4:
        BlobPath = namedtuple("BlobPath", ['stage','company','policy','timestamp'])
        return BlobPath(
            blob_path.parts[0],
            blob_path.parts[1],
            blob_path.parts[2],
            blob_path.stem)
    elif len(blob_path.parts) == 5:
        RunBlobPath = namedtuple("RunBlobPath", ['stage','company','policy','timestamp','run_id'])
        return RunBlobPath(
            blob_path.parts[0],
            blob_path.parts[1],
            blob_path.parts[2],
            blob_path.parts[3],
            blob_path.stem)
    else:
        raise ValueError(f"Invalid path {path}")

def get_blob_service_client() -> BlobServiceClient:
    global _client
    """Get blob service client from connection string environment variable"""
    if _client is not None:
        return _client
    connection_string = os.environ.get(_connection_key)
    if not connection_string:
        raise ValueError(f"{_connection_key} environment variable not set")
    try:
        _client = BlobServiceClient.from_connection_string(connection_string)
        atexit.register(lambda: _client.close())
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

def list_blobs(container=DEFAULT_CONTAINER) -> list[str]:
    client = get_blob_service_client()
    container_client = client.get_container_client(container)
    if not container_client.exists():
        raise RuntimeError("Container does not exist: " + container)
    pages = container_client.list_blob_names()                      
    blobs = []
    for blob in pages:
        blobs.append(blob.removeprefix(f"{container}/"))
    return blobs

    
def list_blobs_nest(container=DEFAULT_CONTAINER) -> dict:
    """Represent container as dictionary."""
    directory = {}  # type: ignore
    for name in list_blobs(container):
        namepath = Path(name)
        subdir = directory
        for i, part in enumerate(namepath.parts):
            if i == len(namepath.parts) - 1:
                leaf = subdir.setdefault(part, None)
            else:
                subdir = subdir.setdefault(part, {})
    return directory

def touch_blobs(stage, company = None, policy = None, timestamp = None, run = None, container=DEFAULT_CONTAINER) -> None:
    blobs = list_blobs_nest()
    for c, policies in blobs[stage].items():
        if company and company != c:
            continue
        for p, timestamps in policies.items():
            if policy and policy != p:
                continue
            for t, runs in timestamps.items():
                if timestamp and not t.startswith(timestamp):
                    continue
                if not runs:
                    path = os.path.join(stage, c, p, t)
                    check_blob(path, container=container, touch=True)
                    continue
                for r in runs:
                    if run and run != r:
                        continue
                    path = os.path.join(stage, c, p, t, r)
                    check_blob(path, container=container, touch=True)

def load_metadata(name, container=DEFAULT_CONTAINER) -> dict:
    def loader(client: BlobClient):
        return client.get_blob_properties().metadata
    return _load_blob(name, loader, container)


def load_blob(name, container=DEFAULT_CONTAINER) -> bytes:
    def loader(client: BlobClient):
        return client.download_blob().readall()
    return _load_blob(name, loader, container)


def _load_blob(name, getter, container=DEFAULT_CONTAINER):
    name = name.removeprefix(f"{container}/")
    logger.debug(f"Downloading blob: {container}/{name}")
    blob_service_client = get_blob_service_client()
    blob_client = blob_service_client.get_blob_client(container=container, blob=name)
    if not blob_client.exists():
        raise ValueError(f"Blob {container}/{name} does not exist!")
    return getter(blob_client)
    
    
def load_json_blob(name, container=DEFAULT_CONTAINER) -> dict:
    data = load_blob(name, container)
    try:
        json_data = json.loads(data.decode('utf-8'))
        return json_data
    except Exception as e:
        logger.error(f"Invalid json blob {name}:\n{e}")
        raise

def load_text_blob(name, container=DEFAULT_CONTAINER) -> str:
    data = load_blob(name, container)
    try:
        txt = data.decode('utf-8')
    except Exception as e:
        logger.error(f"Error decoding text blob {name}:\n{e}")
        raise
    return txt

def upload_blob(data, blob_name, content_type, container=DEFAULT_CONTAINER, metadata=None) -> None:
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
        ),
        metadata=metadata
    )

def upload_text_blob(data, blob_name, container=DEFAULT_CONTAINER, metadata=None) -> None:
    data_bytes = data.encode('utf-8')
    content_type = 'text/plain; charset=utf-8'
    upload_blob(data_bytes, blob_name, content_type, container, metadata)

def upload_json_blob(data: str, blob_name, container=DEFAULT_CONTAINER, metadata=None) -> None:
    data_bytes = data.encode('utf-8')
    content_type = 'application/json; charset=utf-8'
    upload_blob(data_bytes, blob_name, content_type, container, metadata)

def upload_html_blob(cleaned_html, blob_name, container=DEFAULT_CONTAINER, metadata=None) -> None:
    html_bytes = cleaned_html.encode('utf-8')
    content_type = 'text/html; charset=utf-8'
    upload_blob(html_bytes, blob_name, content_type, container, metadata)

def upload_metadata(data, blob_name, container=DEFAULT_CONTAINER) -> None:
    logger.debug(f"Uploading metadata to {container}/{blob_name}")
    if not check_blob(blob_name, container, touch=False):
        return
    blob_service_client = get_blob_service_client()
    blob_client = blob_service_client.get_blob_client(
        container=container, 
        blob=blob_name
    )
    blob_client.set_blob_metadata(data)
    
def remove_blob(blob_name, container=DEFAULT_CONTAINER) -> None:
    if not check_blob(blob_name, container, touch=False):
        return
    logger.debug(f"Deleting blob {blob_name}")
    blob_service_client = get_blob_service_client()
    blob_client = blob_service_client.get_blob_client(
        container=container, 
        blob=blob_name
    )
    blob_client.delete_blob()
    
