from functools import wraps
from typing import Any, Optional, Protocol
from azure.storage.blob import BlobServiceClient

CONTAINER_NAME = 'documents' # nb: can't change this because blob triggers are necessarily hardcoded
DEFAULT_CONNECTION: str = "AzureWebJobsStorage"

class BlobStorageProtocol(Protocol):
    container: str
    key: str
    
    def __init__(self, key=DEFAULT_CONNECTION):
        self.container = CONTAINER_NAME
        self.key = key

    def get_connection_key(self):
        return self.key

    def get_blob_service_client(self) -> BlobServiceClient: ...

    def exists_container(self) -> bool: ...

    def create_container(self) -> None: ...

    def exists_blob(self, blob_name: str) -> bool: ...

    def list_blobs(self) -> list[str]: ...

    def load_metadata(self, blob_name: str) -> dict: ...

    def load_blob(self, blob_name: str) -> bytes: ...

    def upload_blob(self, data: Any, blob_name: str, content_type: str, metadata: Optional[dict] = None) -> None: ...

    def upload_metadata(self, data: dict, blob_name: str) -> None: ...

    def remove_blob(self, blob_name: str) -> None: ...

