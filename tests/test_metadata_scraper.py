import pytest
from unittest.mock import patch, MagicMock, call
from src.metadata_scraper import get_wayback_metadatas, scrape_wayback_metadata
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError, ServiceRequestError, ClientAuthenticationError
from json import JSONDecodeError
import requests

"""
Review these tests. Identify any redundant tests. Identify any other important failure modes or resiliencies that are un-tested. Assess whether they are testing functionally important aspects of the function behavior. Assess whether the tests are over-specified. Assess whether the use of mocks make the tests tautological or informative.

Implement the following recommendations:
* Test the retry logic in scrape_wayback_metadata. It should allow two errors and raise on the third.
* Test the caching behavior of get_wayback_metadata. If the blob exists, the scraper should not be called.
* Edit all exception matching to only match the exception type, not the message.
* Create a new pytest file test_seeder.py which validates the urls. The only validation needed is to run scraper_utils.validate_url and scraper_utils.sanitize_urlpath to ensure a well-formed blob name can be extracted from the urls.
"""

@pytest.fixture
def sample_urls():
    """Sample URL data structure"""
    return {
        "company1": ["https://example1.com", "https://example2.com"],
        "company2": ["https://example3.com", "https://example4.com"]
    }


@pytest.fixture
def mock_metadata_response():
    """Sample Wayback Machine metadata response"""
    return [
        ["urlkey", "timestamp", "original", "mimetype", "statuscode", "digest", "length"],
        ["com,example)/", "20200101000000", "https://example.com/", "text/html", "200", "ABC123", "1234"]
    ]


@pytest.fixture
def mock_requests_response(mock_metadata_response):
    """Mock requests.Response object with json() method"""
    mock_response = MagicMock()
    mock_response.json.return_value = mock_metadata_response
    return mock_response


class TestGetWaybackMetadatas:
    """Tests for get_wayback_metadatas function"""
 
    @patch('src.metadata_scraper.scrape_wayback_metadata')
    @patch('src.metadata_scraper.load_urls')
    def test_all_urls_succeed_processes_all(self, mock_load_urls, mock_get_metadata, sample_urls):
        """Test that all URLs are processed successfully"""
        mock_load_urls.return_value = sample_urls
        mock_get_metadata.return_value = None  # Side effect function
        
        get_wayback_metadatas()
        
        # Verify all 3 URLs were processed
        assert mock_get_metadata.call_count == sum(len(v) for v in sample_urls.values())

    @patch('src.metadata_scraper.scrape_wayback_metadata')
    @patch('src.metadata_scraper.load_urls')
    def test_circuit_fails_three_errors(self, mock_load_urls, mock_get_metadata, sample_urls):
        """Test that function raises after global retry budget (2) is exhausted.
        
        This tests the circuit breaker pattern: if external service starts failing,
        fail fast rather than hammering it with all remaining requests.
        """
        mock_load_urls.return_value = sample_urls
        # All URLs fail with same error (simulating external service degradation)
        mock_get_metadata.side_effect = [
            requests.Timeout("Service degraded"),
            requests.Timeout("Service degraded"),
            requests.Timeout("Service degraded"),
            None
        ]
        
        with pytest.raises(requests.Timeout):
            get_wayback_metadatas()
        
        # Verify circuit breaker: only 3 attempts made (initial + 2 retries)
        assert mock_get_metadata.call_count == 3

    @patch('src.metadata_scraper.scrape_wayback_metadata')
    @patch('src.metadata_scraper.load_urls')
    def test_circuit_passes_two_errors(self, mock_load_urls, mock_get_metadata, sample_urls):
        """Test that single URL failure consumes global retry budget but processing continues"""
        mock_load_urls.return_value = sample_urls
        # First URL fails once (transient), second succeeds
        mock_get_metadata.side_effect = [
            requests.Timeout("Transient error"),
            requests.Timeout("Transient error"),
            None,  # Third URL succeeds
            None,  # Fourth URL succeeds
        ]
        
        get_wayback_metadatas()
        
        # Both URLs attempted
        assert mock_get_metadata.call_count == 4        

class TestScrapeWaybackMetadata:
    """Tests for get_wayback_metadata function with caching behavior"""

    @patch('src.metadata_scraper.requests.get')
    @patch('src.metadata_scraper.check_blob')
    def test_blob_exists_scraper_not_called(self, mock_blob_exists, mock_get):
        """Test that scraper is not called when blob already exists"""
        mock_blob_exists.return_value = True
        
        scrape_wayback_metadata("https://example.com", "company1")
        
        mock_blob_exists.assert_called_once()
        mock_get.assert_not_called()

    @patch('src.metadata_scraper.upload_json_blob')
    @patch('src.metadata_scraper.requests.get')
    @patch('src.metadata_scraper.check_blob')
    def test_blob_not_exists_scraper_called(self, mock_blob_exists, mock_get, mock_upload, mock_requests_response):
        """Test that scraper is called when blob does not exist"""
        mock_blob_exists.return_value = False
        mock_get.return_value = mock_requests_response
        
        scrape_wayback_metadata("https://example.com", "company1")
        
        mock_get.assert_called_once()
        mock_upload.assert_called_once()

    @patch('src.metadata_scraper.upload_json_blob')
    @patch('src.metadata_scraper.time.sleep')
    @patch('src.metadata_scraper.requests.get')
    @patch('src.metadata_scraper.check_blob')
    def test_retry_logic_two_errors_then_success(self, mock_blob_exists, mock_get, mock_sleep, mock_upload, mock_requests_response):
        """Test that function retries twice after errors and succeeds on third attempt"""
        mock_blob_exists.return_value = False
        mock_get.return_value = mock_requests_response

        mock_get.side_effect = [
            requests.Timeout("Error 1"),
            requests.ConnectionError("Error 2"),
            MagicMock(json=lambda: [['data']])
        ]
        
        scrape_wayback_metadata("https://example.com", "company1")
        
        assert mock_get.call_count == 3

    @patch('src.metadata_scraper.time.sleep')
    @patch('src.metadata_scraper.requests.get')
    @patch('src.metadata_scraper.check_blob')
    def test_retry_logic_three_errors_raises(self, mock_blob_exists, mock_get, mock_sleep, mock_requests_response):
        """Test that function raises after three consecutive errors"""
        mock_blob_exists.return_value = False
        mock_get.return_value = mock_requests_response
        mock_get.side_effect = [
            requests.Timeout("Error 1"),
            requests.ConnectionError("Error 2"),
            requests.ConnectionError("Error 3"),
        ]
        
        with pytest.raises(requests.ConnectionError):
            scrape_wayback_metadata("https://example.com", "company1")
        
        assert mock_get.call_count == 3

    @patch('src.metadata_scraper.time.sleep')
    @patch('src.metadata_scraper.requests.get')
    @patch('src.metadata_scraper.check_blob')
    def test_request_failure(self, mock_blob_exists, mock_get, mock_sleep):
        """Test handling of request failure"""
        mock_blob_exists.return_value = False
        mock_get.side_effect = requests.ConnectionError("Persistent error")
        
        with pytest.raises(requests.ConnectionError):
            scrape_wayback_metadata("https://example.com", "company1")

    @patch('src.metadata_scraper.upload_json_blob')
    @patch('src.metadata_scraper.requests.get')
    @patch('src.metadata_scraper.check_blob')
    def test_json_decode_error(self, mock_blob_exists, mock_get, mock_upload, mock_requests_response):
        """Test that scraper is called when blob does not exist"""
        mock_blob_exists.return_value = False
        mock_response = MagicMock()
        mock_response.json.return_value = "{invalid: 'json'}"
        mock_get.return_value = mock_response
        
        with pytest.raises(JSONDecodeError):
            scrape_wayback_metadata("https://example.com", "company1")
