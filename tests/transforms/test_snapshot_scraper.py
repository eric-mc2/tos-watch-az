import pytest
from requests import HTTPError
from src.transforms.snapshot_scraper import SnapshotScraper
from src.adapters.http.fake_client import FakeHttpAdapter, FakeHttpResponse
from src.adapters.storage.fake_client import FakeStorageAdapter
from src.services.blob import BlobService
from src.stages import Stage

@pytest.fixture
def fake_http_client():
    client = FakeHttpAdapter()
    yield client
    client.reset()


@pytest.fixture
def fake_storage():
    adapter = FakeStorageAdapter(container="test-container")
    adapter.create_container()
    service = BlobService(adapter)
    return service


@pytest.fixture
def scraper(fake_storage, fake_http_client):
    return SnapshotScraper(storage=fake_storage, http_client=fake_http_client)


class TestEncodingHandling:
    """Test that encoding detection doesn't corrupt data"""

    def test_windows_1252_encoding_preserves_special_chars(self, scraper, fake_http_client):
        """Test that windows-1252 encoded content is decoded correctly"""
        # Windows-1252 specific characters: smart quotes, em dash, etc.
        original_text = "Microsoft's \u201cTerms of Service\u201d \u2014 updated 2024"
        html_with_special_chars = f"<html><body><main>{original_text}</main></body></html>"
        
        # Encode as windows-1252
        windows_1252_bytes = html_with_special_chars.encode('windows-1252')
        
        response = FakeHttpResponse(status_code=200)
        response.content = windows_1252_bytes
        response.headers = {'content-type': 'text/html; charset=windows-1252'}
        
        html_content, detected_encoding = scraper.decode_html(response)
        
        # Verify the special characters are preserved
        assert "Microsoft's" in html_content or "Microsoft's" in html_content
        assert "\u201c" in html_content or '"' in html_content  # Left double quotation mark
        assert "\u2014" in html_content or "—" in html_content  # Em dash
        assert detected_encoding.lower() == 'windows-1252'


    def test_latin1_fallback_doesnt_crash(self, scraper, fake_http_client):
        """Test that latin1 fallback handles invalid UTF-8"""
        # Create bytes that are valid latin1 but invalid UTF-8
        invalid_utf8 = b"<html><body>\xff\xfe</body></html>"
        
        response = FakeHttpResponse(status_code=200)
        response.content = invalid_utf8
        response.headers = {}
        
        html_content, detected_encoding = scraper.decode_html(response)
        
        # Should not crash and should return something
        assert html_content is not None
        assert len(html_content) > 0


    def test_charset_extracted_from_quotes_in_header(self, scraper):
        """Test charset detection with quoted values in Content-Type header"""
        html_bytes = "<html><body>Test</body></html>".encode('iso-8859-1')
        
        response = FakeHttpResponse(status_code=200)
        response.content = html_bytes
        response.headers = {'content-type': 'text/html; charset="iso-8859-1"'}
        
        html_content, detected_encoding = scraper.decode_html(response)
        
        assert detected_encoding == 'iso-8859-1'
        assert 'Test' in html_content


class TestHtmlExtraction:
    """Test that HTML extraction preserves main content and removes junk"""

    def test_extracts_main_tag_when_present(self, scraper):
        """Test that content inside <main> tag is preserved"""
        html = """
        <html>
        <head><script>tracking();</script></head>
        <body>
            <nav>Navigation</nav>
            <aside class="ad">Advertisement</aside>
            <main>
                <h1>Privacy Policy</h1>
                <p>This is the actual content.</p>
            </main>
            <footer>Copyright 2024</footer>
        </body>
        </html>
        """
        
        result = scraper.extract_main_text(html)
        
        # Main content should be present
        assert "Privacy Policy" in result
        assert "This is the actual content" in result
        
        # Junk should be excluded (when main tag is used, only main is returned)
        assert "Navigation" not in result
        assert "Advertisement" not in result
        assert "Copyright 2024" not in result
        assert "tracking()" not in result


    def test_removes_scripts_and_styles_without_main(self, scraper):
        """Test that scripts/styles are removed when no main tag exists"""
        html = """
        <html>
        <head>
            <style>.ad { display: none; }</style>
        </head>
        <body>
            <script>analytics.track();</script>
            <h1>Terms of Service</h1>
            <p>Important legal text.</p>
            <footer>Footer text</footer>
        </body>
        </html>
        """
        
        result = scraper.extract_main_text(html)
        
        # Content should be present
        assert "Terms of Service" in result
        assert "Important legal text" in result
        
        # Scripts, styles, and footers should be removed
        assert "analytics.track()" not in result
        assert ".ad { display: none; }" not in result
        assert "Footer text" not in result


    def test_removes_ad_containers_by_class(self, scraper):
        """Test that ad containers are removed by class name"""
        html = """
        <html>
        <body>
            <div class="content">Real content</div>
            <div class="ad">Buy now!</div>
            <div class="ads">More ads</div>
            <div class="advertisement">Even more ads</div>
        </body>
        </html>
        """
        
        result = scraper.extract_main_text(html)
        
        assert "Real content" in result
        assert "Buy now!" not in result
        assert "More ads" not in result
        assert "Even more ads" not in result


class TestBlobNaming:
    """Test that blob naming convention stays consistent"""

    def test_get_website_creates_correct_blob_path(self, scraper, fake_http_client, fake_storage):
        """Test blob path format: 02-snapshots/{company}/{policy}/{timestamp}.html"""
        html_content = "<html><body>Test</body></html>"
        response = FakeHttpResponse(status_code=200)
        response.content = html_content.encode('utf-8')
        response.headers = {'content-type': 'text/html; charset=utf-8'}
        
        fake_http_client.configure_default_response(response)
        
        scraper.get_website(
            company="acme",
            policy="privacy",
            timestamp="20240101120000",
            url="https://example.com/privacy"
        )
        
        # Verify the exact blob path format
        expected_blob = f"{Stage.SNAP.value}/acme/privacy/20240101120000.html"
        assert fake_storage.check_blob(expected_blob)


    def test_get_wayback_snapshot_constructs_correct_url_and_blob(self, scraper, fake_http_client, fake_storage):
        """Test wayback snapshot URL construction and blob naming"""
        html_content = "<html><body>Wayback content</body></html>"
        response = FakeHttpResponse(status_code=200)
        response.content = html_content.encode('utf-8')
        response.headers = {}
        
        task_id = "20240115/https://example.com/tos"
        expected_url = f"https://web.archive.org/web/{task_id}"
        
        fake_http_client.configure_response(expected_url, response)
        
        scraper.get_wayback_snapshot(
            company="testco",
            policy="tos",
            timestamp="20240115",
            task_id=task_id
        )
        
        expected_blob = f"{Stage.SNAP.value}/testco/tos/20240115.html"
        assert fake_storage.check_blob(expected_blob)


class TestHttpErrorRetry:
    """Test HTTP error handling and retry logic"""

    def test_404_fails_immediately_no_retry_success(self, scraper, fake_http_client, fake_storage):
        """Test that 404 errors fail both times, don't silently succeed"""
        fake_http_client.configure_error(404)
        
        with pytest.raises(HTTPError):
            scraper.get_website(
                company="testco",
                policy="privacy",
                timestamp="20240101",
                url="https://example.com/missing"
            )
        
        # Blob should not be created
        assert not fake_storage.check_blob(f"{Stage.SNAP.value}/testco/privacy/20240101.html")


    def test_500_error_fails_both_attempts(self, scraper, fake_http_client, fake_storage):
        """Test that 500 errors fail on both attempts"""
        fake_http_client.configure_error(500)
        
        with pytest.raises(HTTPError):
            scraper.get_website(
                company="testco",
                policy="tos",
                timestamp="20240201",
                url="https://example.com/error"
            )


    def test_first_fails_second_succeeds_without_headers(self, scraper, fake_http_client, fake_storage):
        """Test retry logic: fails with headers, succeeds without"""
        # Configure to return error on first call, succeed on second
        fake_http_client.configure_error(403, until_call=1)
        
        success_response = FakeHttpResponse(status_code=200)
        success_response.content = b"<html><body>Success</body></html>"
        success_response.headers = {}
        fake_http_client.configure_default_response(success_response)
        
        # Should not raise exception due to retry
        scraper.get_website(
            company="testco",
            policy="privacy",
            timestamp="20240301",
            url="https://example.com/retry-test"
        )
        
        # Blob should be created from second attempt
        assert fake_storage.check_blob(f"{Stage.SNAP.value}/testco/privacy/20240301.html")
        
        # Verify client was called twice
        assert fake_http_client._call_count == 2


    def test_skips_existing_blob(self, scraper, fake_http_client, fake_storage):
        """Test that existing blobs are skipped without HTTP request"""
        blob_name = f"{Stage.SNAP.value}/existing/policy/20240101.html"
        fake_storage.upload_html_blob("<html>Existing</html>", blob_name)
        
        scraper.get_website(
            company="existing",
            policy="policy",
            timestamp="20240101",
            url="https://example.com/should-not-call"
        )
        
        # HTTP client should not have been called
        assert fake_http_client._call_count == 0


class TestIntegration:
    """Integration tests for complete workflows"""

    def test_end_to_end_scrape_with_complex_html(self, scraper, fake_http_client, fake_storage):
        """Test complete scraping workflow with realistic HTML"""
        complex_html = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="utf-8">
            <script>analytics();</script>
            <style>.hidden { display: none; }</style>
        </head>
        <body>
            <nav>Site Navigation</nav>
            <aside class="advertisement">Ad Space</aside>
            <main>
                <h1>Privacy Policy</h1>
                <section>
                    <h2>Data Collection</h2>
                    <p>We collect the following data...</p>
                </section>
            </main>
            <footer>© 2024 Company</footer>
            <script>moreTracking();</script>
        </body>
        </html>
        """
        
        response = FakeHttpResponse(status_code=200)
        response.content = complex_html.encode('utf-8')
        response.headers = {'content-type': 'text/html; charset=utf-8'}
        fake_http_client.configure_default_response(response)
        
        scraper.get_website(
            company="bigcorp",
            policy="privacy",
            timestamp="20240401120000",
            url="https://bigcorp.example.com/privacy"
        )
        
        blob_name = f"{Stage.SNAP.value}/bigcorp/privacy/20240401120000.html"
        assert fake_storage.check_blob(blob_name)
        
        # Load and verify the cleaned content
        stored_content = fake_storage.load_text_blob(blob_name)
        
        # Main content should be preserved
        assert "Privacy Policy" in stored_content
        assert "Data Collection" in stored_content
        assert "We collect the following data" in stored_content
        
        # Junk should be removed (main tag extracts only main content)
        assert "Site Navigation" not in stored_content
        assert "Ad Space" not in stored_content
        assert "© 2024 Company" not in stored_content
        assert "analytics()" not in stored_content
        assert "moreTracking()" not in stored_content
