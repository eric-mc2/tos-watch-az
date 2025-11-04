import pytest
from unittest.mock import patch, MagicMock, call
from src.metadata_scraper import scrape_wayback_metadata
from src.seeder import URL_DATA
import requests
import json
from tests.test_orchestrator import run_orchestrator, MockDurableOrchestrationContext, rate_limit_config
from pathlib import Path

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

    def test_integration(self):
        config = {
            "meta": {
                "rate_limit_rpm": 300,  # 3 tokens per minute
                "delay": 10,  # Check every 5 seconds
                "activity_name": "process_task",
                "max_retries": 2
            }
        }
        root = Path(__file__).parent.parent.absolute()
        url_path = f"{root}/{URL_DATA}"
        with open(url_path) as f:
            urls = f.read()
        urls = json.loads(urls)
        store = {}
        contexts = []
        for company, url_list in urls.items():
            for url in url_list:
                orchestration_input = {
                    "company": company,
                    "url": url,
                    "task_id": url,
                    "workflow_type": "meta"
                }
                context = MockDurableOrchestrationContext(
                    orchestration_input,
                    store,
                    config
                )
                contexts.append(context)
                try:
                    result = run_orchestrator(context, config)
                except Exception as e:
                    print(f"  [ERROR] {url} failed: {e}")