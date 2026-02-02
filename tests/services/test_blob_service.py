import pytest
import json
from src.container import ServiceContainer

@pytest.fixture
def blob_service():
    """Create a BlobService with FakeStorageAdapter for testing."""
    services = ServiceContainer.create_dev()
    return services.storage


@pytest.fixture
def populated_blob_service(blob_service):
    """Create a BlobService with some test data."""
    # Upload some test blobs
    blob_service.upload_text_blob("test data", "stage1/company1/policy1/2024-01-01.txt")
    blob_service.upload_json_blob(json.dumps({"key": "value"}), "stage1/company1/policy2/2024-01-02.json")
    blob_service.upload_text_blob("run data", "stage1/company2/policy1/2024-01-03/run123.txt")
    blob_service.upload_html_blob("<html><body>Test</body></html>", "stage2/company1/policy1/2024-01-04.html")
    return blob_service


def test_parse_blob_path_four_parts(blob_service):
    """Test parsing a blob path with 4 parts (stage/company/policy/timestamp)."""
    path = "stage1/company1/policy1/2024-01-01.txt"
    parsed = blob_service.parse_blob_path(path)
    
    assert parsed.stage == "stage1"
    assert parsed.company == "company1"
    assert parsed.policy == "policy1"
    assert parsed.timestamp == "2024-01-01"


def test_parse_blob_path_five_parts(blob_service):
    """Test parsing a blob path with 5 parts (includes run_id)."""
    path = "stage1/company1/policy1/2024-01-01/run123.txt"
    parsed = blob_service.parse_blob_path(path)
    
    assert parsed.stage == "stage1"
    assert parsed.company == "company1"
    assert parsed.policy == "policy1"
    assert parsed.timestamp == "2024-01-01"
    assert parsed.run_id == "run123"


def test_list_blobs_nest(populated_blob_service):
    """Test nested blob listing returns proper dictionary structure."""
    result = populated_blob_service.list_blobs_nest()
    
    assert "stage1" in result
    assert "stage2" in result
    assert "company1" in result["stage1"]
    assert "company2" in result["stage1"]
    assert "policy1" in result["stage1"]["company1"]
    assert "policy2" in result["stage1"]["company1"]


def test_check_blob_exists(populated_blob_service):
    """Test checking if a blob exists."""
    exists = populated_blob_service.check_blob("stage1/company1/policy1/2024-01-01.txt")
    assert exists is True
    
    not_exists = populated_blob_service.check_blob("stage1/company1/policy1/2024-99-99.txt")
    assert not_exists is False


def test_check_blob_with_touch(populated_blob_service):
    """Test checking and touching a blob updates metadata."""
    blob_name = "stage1/company1/policy1/2024-01-01.txt"
    
    # First check without touch
    populated_blob_service.check_blob(blob_name, touch=False)
    metadata = populated_blob_service.adapter.load_metadata(blob_name)
    assert "touched" not in metadata
    
    # Check with touch
    populated_blob_service.check_blob(blob_name, touch=True)
    metadata = populated_blob_service.adapter.load_metadata(blob_name)
    assert "touched" in metadata


def test_touch_blobs_all(populated_blob_service):
    """Test touching all blobs in a stage."""
    populated_blob_service.touch_blobs("stage1")
    
    metadata1 = populated_blob_service.adapter.load_metadata("stage1/company1/policy1/2024-01-01.txt")
    assert "touched" in metadata1
    
    metadata2 = populated_blob_service.adapter.load_metadata("stage1/company2/policy1/2024-01-03/run123.txt")
    assert "touched" in metadata2


def test_touch_blobs_filtered_by_company(populated_blob_service):
    """Test touching blobs filtered by company."""
    populated_blob_service.touch_blobs("stage1", company="company1")
    
    metadata1 = populated_blob_service.adapter.load_metadata("stage1/company1/policy1/2024-01-01.txt")
    assert "touched" in metadata1
    
    metadata2 = populated_blob_service.adapter.load_metadata("stage1/company2/policy1/2024-01-03/run123.txt")
    assert "touched" not in metadata2


def test_load_json_blob(populated_blob_service):
    """Test loading a JSON blob."""
    result = populated_blob_service.load_json_blob("stage1/company1/policy2/2024-01-02.json")
    
    assert result == {"key": "value"}


def test_load_text_blob(populated_blob_service):
    """Test loading a text blob."""
    result = populated_blob_service.load_text_blob("stage1/company1/policy1/2024-01-01.txt")
    
    assert result == "test data"


def test_upload_text_blob(blob_service):
    """Test uploading a text blob."""
    blob_service.upload_text_blob("Hello World", "test/file.txt")
    
    assert blob_service.adapter.exists_blob("test/file.txt")
    data = blob_service.load_text_blob("test/file.txt")
    assert data == "Hello World"


def test_upload_json_blob(blob_service):
    """Test uploading a JSON blob."""
    test_data = {"name": "test", "value": 123}
    blob_service.upload_json_blob(json.dumps(test_data), "test/data.json")
    
    assert blob_service.adapter.exists_blob("test/data.json")
    loaded = blob_service.load_json_blob("test/data.json")
    assert loaded == test_data


def test_upload_html_blob(blob_service):
    """Test uploading an HTML blob."""
    html = "<html><body>Test</body></html>"
    blob_service.upload_html_blob(html, "test/page.html")
    
    assert blob_service.adapter.exists_blob("test/page.html")
    loaded = blob_service.load_text_blob("test/page.html")
    assert loaded == html


def test_upload_blob_with_metadata(blob_service):
    """Test uploading a blob with metadata."""
    metadata = {"author": "test", "version": "1.0"}
    blob_service.upload_text_blob("content", "test/meta.txt", metadata=metadata)
    
    loaded_metadata = blob_service.adapter.load_metadata("test/meta.txt")
    assert loaded_metadata["author"] == "test"
    assert loaded_metadata["version"] == "1.0"


def test_upload_metadata(blob_service):
    """Test uploading metadata to an existing blob."""
    blob_service.upload_text_blob("content", "test/file.txt")
    
    metadata = {"updated": "2024-01-01"}
    blob_service.upload_metadata(metadata, "test/file.txt")
    
    loaded_metadata = blob_service.adapter.load_metadata("test/file.txt")
    assert loaded_metadata["updated"] == "2024-01-01"


def test_remove_blob(blob_service):
    """Test removing a blob."""
    blob_service.upload_text_blob("content", "test/delete.txt")
    assert blob_service.adapter.exists_blob("test/delete.txt")
    
    blob_service.remove_blob("test/delete.txt")
    assert not blob_service.adapter.exists_blob("test/delete.txt")
