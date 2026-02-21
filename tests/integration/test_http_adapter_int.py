import pytest
from requests.exceptions import HTTPError
from src.adapters.http.client import RequestsAdapter
from src.transforms.seeds import STATIC_URLS


@pytest.fixture
def prod_http_client():
    client = RequestsAdapter()
    return client


class TestHttpIntegration:
    """Integration tests for end-to-end behavior"""

    @pytest.mark.skip
    def test_successful_scrape(self, prod_http_client):
        """Test successful metadata scraping with caching"""
        success = 0
        fails = 0
        for company, urls in STATIC_URLS.items():
            for url in urls:
                try:
                    resp = prod_http_client.get_and_raise(url)
                    success += 1
                except HTTPError as e:
                    fails += 1
        assert fails == 0, f"Success {success}. Fails {fails}"
            