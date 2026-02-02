import pytest

from src.clients.storage.fake_client import FakeStorageAdapter
from src.services.blob import BlobService
from src.services.seeder import Seeder

@pytest.fixture
def fake_storage():
    adapter = FakeStorageAdapter(container="test-container")
    return BlobService(adapter)

@pytest.fixture
def seeder(fake_storage):
    return Seeder(fake_storage)

@pytest.fixture
def valid_urls():
    """Sample valid URLs"""
    return [
        ("https://example.com","example.com"),
        ("https://www.example.com","example.com"),
        ("https://example.com/path/to/page","page"),
        ("https://trailingslash.com/","trailingslash.com"),
        ("http://subdomain.example.com","subdomain.example.com"),
        ("http://subdomain.example.com/page.html","page"),
        ("https://example.com/path?query=value","path"),
        ("https://example.com:8080/page","page")
    ]


@pytest.fixture
def invalid_urls():
    """Sample invalid URLs"""
    return [
        "not-a-url",
        "javascript:alert('xss')",
        "",
        "//example.com",
        "example.com",
        "www.example.com"
    ]


class TestSeeder:

    def test_valid_urls_pass_validation(self, seeder, valid_urls):
        """Test that valid URLs pass validation"""
        seeder.seed_urls({"corp": [url[0] for url in valid_urls]})

    def test_invalid_urls_fail_validation(self, seeder, invalid_urls):
        """Test that invalid URLs fail validation"""
        with pytest.raises(ValueError):
            seeder.seed_urls({"corp": invalid_urls})
        