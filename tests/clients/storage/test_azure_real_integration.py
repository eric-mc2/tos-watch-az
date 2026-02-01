import pytest
from src.clients.storage.azure import AzureStorageAdapter
from dotenv import load_dotenv

@pytest.fixture
def storage():
    """Create a fresh storage adapter with a test container"""
    load_dotenv()
    
    adapter = AzureStorageAdapter("test-integration-container")
    
    # Create container if it doesn't exist
    if not adapter.exists_container():
        adapter.create_container()
    
    yield adapter
    
    # Cleanup: remove all blobs and delete the container
    try:
        for blob_name in adapter.list_blobs():
            adapter.remove_blob(blob_name)
        
        # Delete the container
        client = adapter.get_blob_service_client()
        container_client = client.get_container_client(adapter.container)
        container_client.delete_container()
    except:
        pass


def test_container_lifecycle(storage):
    """Test checking container existence"""
    assert storage.exists_container()


def test_upload_and_download_blob(storage):
    """Test uploading and downloading blob data"""
    data = b"Hello, Azure!"
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
