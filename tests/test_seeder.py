import pytest
from src import seeder
from unittest.mock import patch
from tests.test_scraper_utils import valid_urls, invalid_urls

class TestSeeder:
    @patch('src.seeder.upload_json_blob')
    def test_valid_urls_pass_validation(self, mock_upload, valid_urls):
        """Test that valid URLs pass validation"""
        seeder.seed_urls({"corp": [url[0] for url in valid_urls]})
    
    
    @patch('src.seeder.upload_json_blob')
    def test_invalid_urls_fail_validation(self, mock_upload, invalid_urls):
        """Test that invalid URLs fail validation"""
        with pytest.raises(ValueError):
            seeder.seed_urls({"corp": invalid_urls})
        