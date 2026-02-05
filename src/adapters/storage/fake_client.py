from typing import Optional

from src.adapters.storage.protocol import BlobStorageProtocol, DEFAULT_CONNECTION, CONTAINER_NAME

class FakeStorageAdapter(BlobStorageProtocol):

    def __init__(self, key: str = DEFAULT_CONNECTION):
        super().__init__(key)
        # In-memory storage: {container: {blob_name: bytes}}
        self._blobs: dict[str, dict[str, bytes]] = {}
        # In-memory metadata: {container: {blob_name: dict}}
        self._metadata: dict[str, dict[str, dict]] = {}

    def get_blob_service_client(self):
        return None  # Not needed for fake

    def exists_container(self) -> bool:
        return self.container in self._blobs

    def create_container(self) -> None:
        self._blobs[self.container] = {}
        self._metadata[self.container] = {}

    def exists_blob(self, blob_name: str) -> bool:
        blob_name = blob_name.removeprefix(f"{self.container}/")
        return blob_name in self._blobs[self.container]


    def list_blobs(self) -> list[str]:
        if self.container not in self._blobs:
            raise RuntimeError("Container does not exist: " + self.container)
        return [name.removeprefix(f"{self.container}/") for name in self._blobs[self.container].keys()]


    def load_metadata(self, blob_name: str) -> dict:
        blob_name = blob_name.removeprefix(f"{self.container}/")
        if blob_name not in self._blobs[self.container]:
            raise ValueError(f"Blob {self.container}/{blob_name} does not exist!")
        return self._metadata[self.container].get(blob_name, {})

    def load_blob(self, blob_name: str) -> bytes:
        blob_name = blob_name.removeprefix(f"{self.container}/")
        if blob_name not in self._blobs[self.container]:
            raise ValueError(f"Blob {self.container}/{blob_name} does not exist!")
        return self._blobs[self.container][blob_name]

    def upload_blob(self, data: bytes, blob_name: str, content_type: str, metadata: Optional[dict] = None) -> None:
        blob_name = blob_name.removeprefix(f"{self.container}/")
        self._blobs[self.container][blob_name] = data
        if metadata:
            self.upload_metadata(metadata, blob_name)

    def upload_metadata(self, data: dict, blob_name: str) -> None:
        blob_name = blob_name.removeprefix(f"{self.container}/")
        self._metadata[self.container][blob_name] = data.copy()

    def remove_blob(self, blob_name: str) -> None:
        blob_name = blob_name.removeprefix(f"{self.container}/")
        del self._blobs[self.container][blob_name]
        if blob_name in self._metadata[self.container]:
            del self._metadata[self.container][blob_name]

