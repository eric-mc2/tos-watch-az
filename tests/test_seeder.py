import pytest
from src import seeder

@pytest.fixture
def valid_urls():
    """Sample valid URLs"""
    return [
        ("https://example.com","index"),
        ("https://example.com/path/to/page","page"),
        ("http://subdomain.example.com/page.html","page"),
        ("https://example.com/path?query=value","path"),
        ("https://example.com:8080/page","page")
    ]


@pytest.fixture
def invalid_urls():
    """Sample invalid URLs"""
    return [
        "not-a-url",
        "ftp://example.com",
        "javascript:alert('xss')",
        "",
        "//example.com"
    ]

class TestSeeder:
    def test_invalid_urls_fail_validation(self, invalid_urls):
        """Test that invalid URLs fail validation"""
        with pytest.raises(ValueError):
            seeder.process_urls({"corp": invalid_urls})
        