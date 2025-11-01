import pytest
from unittest.mock import patch, MagicMock, call
from src.metadata_scraper import get_wayback_metadatas, scrape_wayback_metadata, get_wayback_metadata
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError, ServiceRequestError, ClientAuthenticationError

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
        "company2": ["https://example3.com"]
    }


@pytest.fixture
def mock_metadata_response():
    """Sample Wayback Machine metadata response"""
    return [
        ["urlkey", "timestamp", "original", "mimetype", "statuscode", "digest", "length"],
        ["com,example)/", "20200101000000", "https://example.com/", "text/html", "200", "ABC123", "1234"]
    ]


class TestGetWaybackMetadatas:
    """Tests for get_wayback_metadatas function"""

    @pytest.mark.parametrize("exception_type,message", [
        (ResourceNotFoundError, "Not found"),
        (HttpResponseError, "Not authorized"),
        (ClientAuthenticationError, "Auth failed"),
        (ServiceRequestError, "Connection timeout")
    ])
    @patch('src.metadata_scraper.load_urls')
    def test_azure_exceptions_propagate(self, mock_load_urls, exception_type, message):
        """All Azure SDK exceptions should propagate correctly"""
        mock_load_urls.side_effect = exception_type(message)
        
        with pytest.raises(exception_type):
            get_wayback_metadatas()
            

    @patch('src.metadata_scraper.time.sleep')
    @patch('src.metadata_scraper.get_wayback_metadata')
    @patch('src.metadata_scraper.load_urls')
    def test_three_urls_error(self, mock_load_urls, mock_get_metadata, mock_sleep):
        """Test that unhandled exception is raised after retries exhausted"""
        mock_load_urls.return_value = {"company1": ["https://example1.com", "https://example2.com", "https://example3.com", "https://example4.com"]}
        mock_get_metadata.side_effect = Exception("Unhandled error")
        
        with pytest.raises(Exception):
            get_wayback_metadatas()
        
        # Should fail on first URL, retry once (2nd call), then retry once more (3rd call), then raise
        expected_calls = [
            call("https://example1.com", 'company1', 'documents'),
            call("https://example2.com", 'company1', 'documents'),
            call("https://example3.com", 'company1', 'documents')
        ]
        mock_get_metadata.assert_has_calls(expected_calls)

    @patch('src.metadata_scraper.time.sleep')
    @patch('src.metadata_scraper.get_wayback_metadata')
    @patch('src.metadata_scraper.load_urls')
    def test_two_urls_error(self, mock_load_urls, mock_get_metadata, mock_sleep):
        """Positive test: first two URLs each error once but succeed on second try"""
        mock_load_urls.return_value = {
            "company1": ["https://example1.com", "https://example2.com"],
            "company2": ["https://example3.com"]
        }
        mock_get_metadata.side_effect = [
            Exception("Error 1"),  # First URL, attempt 1 (retries=2)
            Exception("Error 2"),  # Second URL, attempt 1 (retries=1)
            None                   # Third URL succeeds
        ]
        
        get_wayback_metadatas()
        
        # First URL tried 2 times, second URL tried 2 times, third URL tried once
        expected_calls = [
            call("https://example1.com", 'company1', 'documents'),
            call("https://example2.com", 'company1', 'documents'),
            call("https://example3.com", 'company2', 'documents')
        ]
        mock_get_metadata.assert_has_calls(expected_calls)

    @patch('src.metadata_scraper.time.sleep')
    @patch('src.metadata_scraper.get_wayback_metadata')
    @patch('src.metadata_scraper.load_urls')
    def test_one_url_errors(self, mock_load_urls, mock_get_metadata, mock_sleep, sample_urls):
        """Test that function retries once and continues processing subsequent URLs"""
        mock_load_urls.return_value = sample_urls
        
        # First URL fails, second succeeds, third succeeds
        mock_get_metadata.side_effect = [
            Exception("Temporary error"),  # First URL, first attempt
            None,  # Second URL succeeds
            None   # Third URL succeeds
        ]
        
        get_wayback_metadatas()
                
        # Verify the correct URLs were called
        expected_calls = [
            call("https://example1.com", 'company1', 'documents'),
            call("https://example2.com", 'company1', 'documents'),
            call("https://example3.com", 'company2', 'documents')
        ]
        mock_get_metadata.assert_has_calls(expected_calls)

    @patch('src.metadata_scraper.get_wayback_metadata')
    @patch('src.metadata_scraper.load_urls')
    def test_all_urls_succeed(self, mock_load_urls, mock_get_metadata, sample_urls):
        """Test successful processing of all URLs"""
        mock_load_urls.return_value = sample_urls
        mock_get_metadata.return_value = None
        
        get_wayback_metadatas()
        
        # Verify all URLs were processed
        expected_calls = [
                    call("https://example1.com", 'company1', 'documents'),
                    call("https://example2.com", 'company1', 'documents'),
                    call("https://example3.com", 'company2', 'documents')
                ]
        mock_get_metadata.assert_has_calls(expected_calls)

class TestGetWaybackMetadata:
    """Tests for get_wayback_metadata function with caching behavior"""

    @patch('src.metadata_scraper.scrape_wayback_metadata')
    @patch('src.metadata_scraper.check_blob')
    def test_blob_exists_scraper_not_called(self, mock_blob_exists, mock_scraper):
        """Test that scraper is not called when blob already exists"""
        mock_blob_exists.return_value = True
        
        get_wayback_metadata("https://example.com", "company1", "documents")
        
        mock_blob_exists.assert_called_once()
        mock_scraper.assert_not_called()

    @patch('src.metadata_scraper.upload_json_blob')
    @patch('src.metadata_scraper.scrape_wayback_metadata')
    @patch('src.metadata_scraper.check_blob')
    def test_blob_not_exists_scraper_called(self, mock_blob_exists, mock_scraper, mock_upload):
        """Test that scraper is called when blob does not exist"""
        mock_blob_exists.return_value = False
        mock_scraper.return_value = [["urlkey", "timestamp"]]
        
        get_wayback_metadata("https://example.com", "company1", "documents")
        
        mock_blob_exists.assert_called_once()
        mock_scraper.assert_called_once_with("https://example.com")


class TestScrapeWaybackMetadata:
    """Tests for scrape_wayback_metadata function"""

    @patch('src.metadata_scraper.requests.get')
    def test_successful_scrape(self, mock_get, mock_metadata_response):
        """Test successful metadata scraping"""
        mock_response = MagicMock()
        mock_response.json.return_value = mock_metadata_response
        mock_get.return_value = mock_response
        
        result = scrape_wayback_metadata("https://example.com")
        
        assert result == mock_metadata_response
        mock_get.assert_called_once()

    @patch('src.metadata_scraper.time.sleep')
    @patch('src.metadata_scraper.requests.get')
    def test_retry_logic_two_errors_then_success(self, mock_get, mock_sleep, mock_metadata_response):
        """Test that function retries twice after errors and succeeds on third attempt"""
        mock_response = MagicMock()
        mock_response.json.return_value = mock_metadata_response
        
        mock_get.side_effect = [
            Exception("Error 1"),
            Exception("Error 2"),
            mock_response
        ]
        
        result = scrape_wayback_metadata("https://example.com")
        
        assert result == mock_metadata_response
        assert mock_get.call_count == 3

    @patch('src.metadata_scraper.time.sleep')
    @patch('src.metadata_scraper.requests.get')
    def test_retry_logic_three_errors_raises(self, mock_get, mock_sleep):
        """Test that function raises after three consecutive errors"""
        mock_get.side_effect = [
            Exception("Error 1"),
            Exception("Error 2"),
            Exception("Error 3")
        ]
        
        with pytest.raises(Exception):
            scrape_wayback_metadata("https://example.com")
        
        assert mock_get.call_count == 3

    @patch('src.metadata_scraper.time.sleep')
    @patch('src.metadata_scraper.requests.get')
    def test_request_failure(self, mock_get, mock_sleep):
        """Test handling of request failure"""
        mock_get.side_effect = Exception("Persistent error")
        
        with pytest.raises(Exception):
            scrape_wayback_metadata("https://example.com")

    @patch('src.metadata_scraper.requests.get')
    def test_json_decode_error(self, mock_get):
        """Test handling of JSON decode error"""
        mock_response = MagicMock()
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_get.return_value = mock_response
        
        with pytest.raises(ValueError):
            scrape_wayback_metadata("https://example.com")
