import pytest
import requests
import json
from src.transforms.metadata_scraper import MetadataScraper
from src.services.blob import BlobService
from src.adapters.http.fake_client import FakeHttpAdapter, FakeHttpResponse
from src.adapters.storage.fake_client import FakeStorageAdapter
from src.stages import Stage


@pytest.fixture
def fake_http_client():
    """Provides a configurable fake HTTP client"""
    client = FakeHttpAdapter()
    yield client
    client.reset()


@pytest.fixture
def fake_blob_service():
    """Fake blob service using FakeStorageAdapter"""
    adapter = FakeStorageAdapter()
    adapter.create_container()
    return BlobService(adapter)


@pytest.fixture
def metadata_scraper(fake_blob_service, fake_http_client):
    """MetadataScraper with fake dependencies"""
    return MetadataScraper(
        storage=fake_blob_service,
        http_client=fake_http_client
    )


@pytest.fixture
def sample_wayback_metadata():
    """Sample Wayback metadata response"""
    return [
        ["urlkey", "timestamp", "original", "mimetype", "statuscode", "digest", "length"],
        ["com,example)/", "20200101000000", "https://example.com/", "text/html", "200", "ABC123", "1234"],
        ["com,example)/page", "20210101000000", "https://example.com/page", "text/html", "200", "DEF456", "5678"]
    ]


class TestMetadataScraperIntegration:
    """Integration tests for end-to-end behavior"""
    
    def test_successful_metadata_scrape_and_cache(self, metadata_scraper, fake_http_client, 
                                                   fake_blob_service, sample_wayback_metadata):
        """Test successful metadata scraping with caching"""
        fake_http_client.configure_default_response(
            FakeHttpResponse(status_code=200, json_data=sample_wayback_metadata)
        )
        
        metadata_scraper.scrape_wayback_metadata("https://example.com", "company1")
        
        # Verify blob was uploaded
        assert fake_blob_service.check_blob(f"{Stage.META.value}/company1/example.com/metadata.json")
        assert fake_http_client._call_count == 1
        
    def test_graceful_failure_on_connection_error(self, metadata_scraper, fake_http_client):
        """Test graceful failure when connection fails"""
        fake_http_client.configure_error(503)
        
        with pytest.raises(requests.HTTPError):
            metadata_scraper.scrape_wayback_metadata("https://example.com", "company1")
    
    def test_cache_hit_skips_scraping(self, metadata_scraper, fake_http_client, 
                                     fake_blob_service, sample_wayback_metadata):
        """Test that cache hit prevents HTTP request"""
        # Pre-populate cache
        blob_name = f"{Stage.META.value}/company1/example.com/metadata.json"
        fake_blob_service.upload_json_blob(json.dumps(sample_wayback_metadata), blob_name)
        
        metadata_scraper.scrape_wayback_metadata("https://example.com", "company1")
        
        # HTTP client should never be called
        assert fake_http_client._call_count == 0


class TestMetadataScraperEdgeCases:
    """Tests for edge cases and error conditions"""
    
    def test_json_decode_error(self, metadata_scraper, fake_http_client):
        """Test handling of invalid JSON response"""
        fake_http_client.configure_default_response(
            FakeHttpResponse(status_code=200, text="Scheduled Maintenance")
        )
        
        with pytest.raises(Exception):
            metadata_scraper.scrape_wayback_metadata("https://example.com", "company1")


class TestMetadataParser:
    """Tests for parse_wayback_metadata"""
    
    def test_parse_valid_metadata(self, metadata_scraper, fake_blob_service, sample_wayback_metadata):
        """Test parsing valid metadata"""
        input_blob = f"{Stage.META.value}/company1/example.com/metadata.json"
        fake_blob_service.upload_json_blob(json.dumps(sample_wayback_metadata), input_blob)
        
        result = metadata_scraper.parse_wayback_metadata("company1", "example.com")
        
        assert len(result) == 2
        assert all('timestamp' in snap for snap in result)
    
    def test_parse_filters_invalid_snapshots(self, metadata_scraper, fake_blob_service):
        """Test that invalid snapshots are filtered out"""
        metadata = [
            ["urlkey", "timestamp", "original", "mimetype", "statuscode", "digest", "length"],
            ["com,example)/", "20200101000000", "https://example.com/", "text/html", "200", "ABC123", "1234"],
            ["com,example)/404", "", "https://example.com/404", "text/html", "404", "DEF456", "5678"],
            ["com,example)/403", "20200101000000", "https://example.com/403", "text/html", "403", "GHI789", "910"]
        ]
        input_blob = f"{Stage.META.value}/company1/example.com/metadata.json"
        fake_blob_service.upload_json_blob(json.dumps(metadata), input_blob)
        
        result = metadata_scraper.parse_wayback_metadata("company1", "example.com")
        
        assert len(result) == 1
        assert result[0]['statuscode'] == '200'
