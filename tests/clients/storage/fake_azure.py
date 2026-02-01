from src.clients.storage.protocol import BlobStorageProtocol

DEFAULT_CONNECTION = "FAKE"

class FakeStorageAdapter(BlobStorageProtocol):

    def __init__(self, container: str, key: str = DEFAULT_CONNECTION):
        super().__init__(container, key)
        # In-memory storage: {container: {blob_name: bytes}}
        self._blobs: dict[str, dict[str, bytes]] = {}
        # In-memory metadata: {container: {blob_name: dict}}
        self._metadata: dict[str, dict[str, dict]] = {}
        self.container = container


    def get_connection_key(self) -> str:
        return self.key

    def get_blob_service_client(self):
        return None  # Not needed for fake

    def exists_container(self) -> bool:
        return self.container in self._blobs

    def create_container(self) -> None:
        self._blobs[self.container] = {}
        self._metadata[self.container] = {}

    def exists_blob(self, blob_name: str) -> bool:
        return blob_name in self._blobs[self.container]


    def list_blobs(self) -> list[str]:
        if self.container not in self._blobs:
            raise RuntimeError("Container does not exist: " + self.container)
        return [name.removeprefix(f"{self.container}/") for name in self._blobs[self.container].keys()]


    def load_metadata(self, name: str) -> dict:
        if name not in self._blobs[self.container]:
            raise ValueError(f"Blob {self.container}/{name} does not exist!")
        return self._metadata[self.container].get(name, {})

    def load_blob(self, name: str) -> bytes:
        if name not in self._blobs[self.container]:
            raise ValueError(f"Blob {self.container}/{name} does not exist!")
        return self._blobs[self.container][name]

    def upload_blob(self, data: bytes, blob_name: str, content_type: str, metadata: dict = None) -> None:
        self._blobs[self.container][blob_name] = data
        if metadata:
            self.upload_metadata(metadata, blob_name)

    def upload_metadata(self, data: dict, blob_name: str) -> None:
        self._metadata[self.container][blob_name] = data.copy()

    def remove_blob(self, blob_name: str) -> None:
        del self._blobs[self.container][blob_name]
        if blob_name in self._metadata[self.container]:
            del self._metadata[self.container][blob_name]

