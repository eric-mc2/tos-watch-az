import pytest

from src.transforms.seeds import STATIC_URLS
from src.utils import path_utils


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


class TestSeeds:
    def test_seed_urls(self):
        for url in STATIC_URLS:
            assert path_utils.validate_url(url)
            assert path_utils.extract_policy(url)

class TestURLValidation:
    """Tests for URL validation and sanitization"""

    def test_valid_urls_produce_blob_names(self, valid_urls):
        """Test that valid URLs can be validated and sanitized to produce blob names"""
        for url, expected_name in valid_urls:
            # URL should be valid
            assert path_utils.validate_url(url)
            
            # URL should produce a well-formed blob name
            blob_name = path_utils.extract_policy(url)
            assert blob_name == expected_name

    def test_invalid_urls_fail_validation(self, invalid_urls):
        """Test that invalid URLs fail validation"""
        for url in invalid_urls:
            # Invalid URLs should fail validation
            is_valid = path_utils.validate_url(url)
            assert not is_valid, url
