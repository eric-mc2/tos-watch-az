import os
import atexit
from typing import Optional, Callable, List, Iterable

from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings
from src.adapters.storage.protocol import BlobStorageProtocol, DEFAULT_CONNECTION

_client : Optional[BlobServiceClient] = None

class AzureStorageAdapter(BlobStorageProtocol):

    def __init__(self, key: str = DEFAULT_CONNECTION):
        super().__init__(key)
        global _client
        if _client is not None:
            _client.close()
            _client = None

    @staticmethod
    def _convert_metadata_for_azure(metadata: Optional[dict]) -> Optional[dict]:
        """Convert metadata values to strings for Azure"""
        if not metadata:
            return metadata
        return {k: str(v) if v is not None else v for k, v in metadata.items()}

    @staticmethod
    def _unconvert_metadata_from_azure(metadata: Optional[dict]) -> Optional[dict]:
        """Unconvert metadata from strings."""
        if not metadata:
            return metadata
        result = {}
        for k, v in metadata.items():
            if v == "True":
                result[k] = True
            elif v == "False":
                result[k] = False
            else:
                result[k] = v
        return result

    def get_blob_service_client(self) -> BlobServiceClient:
        global _client
        """Get blob service client from connection string environment variable"""
        if _client is not None:
            return _client
        connection_string = os.environ.get(self.key)
        if not connection_string:
            raise ValueError(f"{self.key} environment variable not set")
        try:
            _client = BlobServiceClient.from_connection_string(connection_string)
            atexit.register(lambda: _client.close())
        except Exception as e:
            raise ConnectionError(f"Failed to create BlobServiceClient:\n{e}") from e
        return _client


    # Container Ops
    def exists_container(self) -> bool:
        client = self.get_blob_service_client()
        container_client = client.get_container_client(self.container)
        return container_client.exists()


    def create_container(self) -> None:
        client = self.get_blob_service_client()
        container_client = client.get_container_client(self.container)
        container_client.create_container()


    # Read Ops
    def exists_blob(self, blob_name: str) -> bool:
        blob_name = blob_name.removeprefix(f"{self.container}/")
        client = self.get_blob_service_client()
        container_client = client.get_container_client(self.container)
        blob_client = container_client.get_blob_client(blob_name)
        return blob_client.exists()


    def list_blobs(self) -> list[str]:
        client = self.get_blob_service_client()
        container_client = client.get_container_client(self.container)
        if not container_client.exists():
            raise RuntimeError("Container does not exist: " + self.container)
        pages = container_client.list_blob_names()
        blobs = []
        for blob in pages:
            blobs.append(blob.removeprefix(f"{self.container}/"))
        return blobs
    

    def list_blobs_by_tag(self, query: Iterable[tuple[str,str]]) -> list[str]:
        client = self.get_blob_service_client()
        container_client = client.get_container_client(self.container)
        if not container_client.exists():
            raise RuntimeError("Container does not exist: " + self.container)

        def filter_expr(key: str, val:str) -> str:
            return f"\"{key}\"='{val}'"

        query_str = " and ".join((filter_expr(key,val) for key,val in query))
        pages = container_client.find_blobs_by_tags(query_str)
        raise NotImplementedError()
        blobs = []
        for blob in pages:
            print(blob)  # what is blob?
            pass
            # blobs.append(blob.removeprefix(f"{self.container}/"))
        return blobs
    

    def load_metadata(self, blob_name: str) -> dict:
        def loader(client: BlobClient):
            metadata = client.get_blob_properties().metadata
            return self._unconvert_metadata_from_azure(metadata)
        return self._load_blob(blob_name, loader)


    def load_blob(self, blob_name: str) -> bytes:
        def loader(client: BlobClient):
            return client.download_blob().readall()
        return self._load_blob(blob_name, loader)


    def _load_blob(self, blob_name: str, getter: Callable):
        blob_name = blob_name.removeprefix(f"{self.container}/")
        blob_service_client = self.get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(container=self.container, blob=blob_name)
        if not blob_client.exists():
            raise ValueError(f"Blob {self.container}/{blob_name} does not exist!")
        return getter(blob_client)


    # Write Ops
    def upload_blob(self, data, blob_name: str, content_type: str, metadata=None) -> None:
        blob_name = blob_name.removeprefix(f"{self.container}/")
        blob_service_client = self.get_blob_service_client()
        # Upload to blob storage with explicit UTF-8 encoding
        blob_client = blob_service_client.get_blob_client(
            container=self.container,
            blob=blob_name
        )
        # Convert metadata values to strings (Azure requires string values)
        metadata = self._convert_metadata_for_azure(metadata)
        
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

    def upload_metadata(self, data, blob_name: str) -> None:
        blob_name = blob_name.removeprefix(f"{self.container}/")
        blob_service_client = self.get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=self.container,
            blob=blob_name
        )
        data = self._convert_metadata_for_azure(data)
        blob_client.set_blob_metadata(data)


    # Delete Ops
    def remove_blob(self, blob_name: str) -> None:
        blob_name = blob_name.removeprefix(f"{self.container}/")
        blob_service_client = self.get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=self.container,
            blob=blob_name
        )
        blob_client.delete_blob()

