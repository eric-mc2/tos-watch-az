import pytest
from requests.exceptions import HTTPError
from src.transforms.snapshot_scraper import SnapshotScraper
from src.services.blob import BlobService
from src.adapters.http.client import RequestsAdapter
from src.adapters.storage.fake_client import FakeStorageAdapter
from src.stages import Stage
from src.transforms.seeds import STATIC_URLS
from src.utils.path_utils import extract_policy

@pytest.fixture
def prod_http_client():
    client = RequestsAdapter()
    return client


class TestHttpIntegration:
    """Integration tests for end-to-end behavior"""

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
            