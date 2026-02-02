import pytest
from src.services.metadata_scraper import MetadataScraper
from src.services.blob import BlobService
from src.clients.http.client import RequestsAdapter
from src.clients.storage.fake_client import FakeStorageAdapter
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
    adapter = FakeStorageAdapter(container='test-container')
    adapter.create_container()
    return BlobService(adapter)


@pytest.fixture
def metadata_scraper(fake_blob_service, prod_http_client):
    """MetadataScraper with fake/real dependencies"""
    return MetadataScraper(
        storage=fake_blob_service,
        http_client=prod_http_client,
    )

class TestMetadataScraperIntegration:
    """Integration tests for end-to-end behavior"""

    def test_successful_metadata_scrape_and_cache(self, metadata_scraper):
        """Test successful metadata scraping with caching"""

        url = STATIC_URLS["google"][0]
        metadata_scraper.scrape_wayback_metadata(url, "company1")

        # Verify blob was uploaded
        policy = extract_policy(url)
        assert metadata_scraper.storage.check_blob(f"{Stage.META.value}/company1/{policy}/metadata.json")
