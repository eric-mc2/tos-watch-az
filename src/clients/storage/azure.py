import os
import atexit
from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings
from src.clients.storage.protocol import BlobStorageProtocol

DEFAULT_CONNECTION = "AzureWebJobsStorage"
_client : BlobServiceClient = None

class AzureStorageAdapter(BlobStorageProtocol):

    def __init__(self, container: str, key: str = DEFAULT_CONNECTION):
        super().__init__(container, key)
        global _client
        if _client:
            _client.close()
            _client = None


    # Connection management
    def get_connection_key(self):
        return self.key


    def get_blob_service_client(self, ) -> BlobServiceClient:
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
    def exists_blob(self, blob_name) -> bool:
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


    def load_metadata(self, name) -> dict:
        def loader(client: BlobClient):
            return client.get_blob_properties().metadata

        return self._load_blob(name, loader)


    def load_blob(self, name) -> bytes:
        def loader(client: BlobClient):
            return client.download_blob().readall()

        return self._load_blob(name, loader)


    def _load_blob(self, name, getter):
        blob_service_client = self.get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(container=self.container, blob=name)
        if not blob_client.exists():
            raise ValueError(f"Blob {self.container}/{name} does not exist!")
        return getter(blob_client)


    # Write Ops
    def upload_blob(self, data, blob_name, content_type, metadata=None) -> None:
        blob_service_client = self.get_blob_service_client()
        # Upload to blob storage with explicit UTF-8 encoding
        blob_client = blob_service_client.get_blob_client(
            container=self.container,
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


    def upload_metadata(self, data, blob_name) -> None:
        blob_service_client = self.get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=self.container,
            blob=blob_name
        )
        blob_client.set_blob_metadata(data)


    # Delete Ops
    def remove_blob(self, blob_name) -> None:
        blob_service_client = self.get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=self.container,
            blob=blob_name
        )
        blob_client.delete_blob()

