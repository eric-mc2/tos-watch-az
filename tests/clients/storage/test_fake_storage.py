import pytest
from src.clients.storage.fake_client import FakeStorageAdapter


@pytest.fixture
def storage():
    """Create a fresh storage adapter for each test"""
    adapter = FakeStorageAdapter(container="test-container")
    adapter.create_container()
    return adapter


def test_container_lifecycle(storage):
    """Test creating and checking container existence"""
    assert storage.exists_container()


def test_upload_and_download_blob(storage):
    """Test uploading and downloading blob data"""
    data = b"Hello, World!"
    blob_name = "test.txt"
    
    storage.upload_blob(data, blob_name, content_type="text/plain")
    
    assert storage.exists_blob(blob_name)
    downloaded = storage.load_blob(blob_name)
    assert downloaded == data


def test_upload_blob_with_metadata(storage):
    """Test uploading blob with metadata"""
    data = b"test data"
    blob_name = "metadata-test.txt"
    metadata = {"author": "test", "version": "1.0"}
    
    storage.upload_blob(data, blob_name, content_type="text/plain", metadata=metadata)
    
    loaded_metadata = storage.load_metadata(blob_name)
    assert loaded_metadata == metadata


def test_list_blobs(storage):
    """Test listing all blobs in container"""
    storage.upload_blob(b"data1", "blob1.txt", "text/plain")
    storage.upload_blob(b"data2", "blob2.txt", "text/plain")
    storage.upload_blob(b"data3", "blob3.txt", "text/plain")
    
    blobs = storage.list_blobs()
    
    assert len(blobs) == 3
    assert "blob1.txt" in blobs
    assert "blob2.txt" in blobs
    assert "blob3.txt" in blobs


def test_remove_blob(storage):
    """Test removing a blob"""
    blob_name = "to-delete.txt"
    storage.upload_blob(b"delete me", blob_name, "text/plain", metadata={"key": "value"})
    
    assert storage.exists_blob(blob_name)
    
    storage.remove_blob(blob_name)
    
    assert not storage.exists_blob(blob_name)
