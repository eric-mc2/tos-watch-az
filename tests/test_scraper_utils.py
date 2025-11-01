import pytest
from src import scraper_utils


@pytest.fixture
def valid_urls():
    """Sample valid URLs"""
    return [
        ("example.com", "example.com"),
        ("www.example.com", "example.com"),
        ("https://example.com","example.com"),
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
        "https://example .com/page",  # Space in domain
        "https://example.com /page",  # Space after domain
        "https:// example.com/page",  # Space after scheme
    ]


class TestURLValidation:
    """Tests for URL validation and sanitization"""

    def test_valid_urls_produce_blob_names(self, valid_urls):
        """Test that valid URLs can be validated and sanitized to produce blob names"""
        for url, expected_name in valid_urls:
            # URL should be valid
            assert scraper_utils.validate_url(url) is True
            
            # URL should produce a well-formed blob name
            blob_name = scraper_utils.sanitize_urlpath(url)
            assert blob_name == expected_name

    def test_invalid_urls_fail_validation(self, invalid_urls):
        """Test that invalid URLs fail validation"""
        for url in invalid_urls:
            # Invalid URLs should fail validation
            is_valid = scraper_utils.validate_url(url)
            assert is_valid is False, url
