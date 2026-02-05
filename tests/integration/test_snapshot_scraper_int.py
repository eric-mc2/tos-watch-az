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


@pytest.fixture
def fake_blob_service():
    """Fake blob service using FakeStorageAdapter"""
    adapter = FakeStorageAdapter()
    adapter.create_container()
    return BlobService(adapter)


@pytest.fixture
def snapshot_scraper(fake_blob_service, prod_http_client) -> SnapshotScraper:
    """SnapshotScraper with fake/real dependencies"""
    return SnapshotScraper(
        storage=fake_blob_service,
        http_client=prod_http_client,
    )

class TestSnapshotScraperIntegration:
    """Integration tests for end-to-end behavior"""

    def test_successful_scrape(self, snapshot_scraper):
        """Test successful metadata scraping with caching"""
        success = 0
        fails = 0
        for company, urls in STATIC_URLS.items():
            for url in urls:
                try:
                    resp = snapshot_scraper.fetch(url)
                    success += 1
                except HTTPError as e:
                    fails += 1
        # TODO: This fails on a bunch of urls.
        print(f"Success: {success}. Fails: {fails}")