import logging
from dataclasses import dataclass
from requests import HTTPError
from bs4 import BeautifulSoup
import chardet  # Add this import for encoding detection

from src.clients.http.protocol import HttpProtocol
from src.utils.log_utils import setup_logger
from src.services.blob import BlobService
from src.stages import Stage

logger = setup_logger(__name__, logging.INFO)

@dataclass
class SnapshotScraper:
    storage: BlobService
    http_client: HttpProtocol

    @staticmethod
    def decode_html(resp):
        # Handle encoding properly
        # First, try to detect the actual encoding from the response
        import re

        detected_encoding = None
        if resp.headers.get('content-type'):
            content_type = resp.headers['content-type'].lower()
            if 'charset=' in content_type:
                # detected_encoding = content_type.split('charset=')[1].split(';')[0].strip()
                match = re.search(r'charset=["\']?([^\s;"\']+)', content_type)
                if match:
                    detected_encoding = match.group(1)

        # If no encoding in headers, try to detect from content
        if not detected_encoding:
            detected = chardet.detect(resp.content[:10000])  # Check first 10KB
            detected_encoding = detected.get('encoding') if detected else None

        # Get the content with proper encoding
        try:
            if detected_encoding:
                html_content = resp.content.decode(detected_encoding)
            else:
                html_content = resp.text  # Let requests handle it
        except (UnicodeDecodeError, LookupError):
            # Fallback to response.text with error handling
            try:
                html_content = resp.content.decode('utf-8', errors='replace')
            except:
                html_content = resp.content.decode('latin1', errors='replace')

        return html_content, detected_encoding


    @staticmethod
    def extract_main_text(html_content, encoding='utf-8'):
        """Extract main content from HTML with proper encoding handling"""
        # Parse with BeautifulSoup, explicitly handling encoding
        soup = BeautifulSoup(html_content, "html.parser", from_encoding=encoding)

        # Try to find the main content; fallback to body text
        main = soup.find('main')
        if main:
            return main.prettify()
        # Remove scripts, styles, footers, sidebars, and ads
        for tag in soup(['script', 'style', 'footer', 'aside', 'nav']):
            tag.decompose()
        # Optionally remove common ad containers
        for ad_tag in soup.find_all(class_=['ad', 'ads', 'advertisement']):
            ad_tag.decompose()
        body = soup.body
        return body.prettify() if body else soup.prettify()


    def get_wayback_snapshot(self, company, policy, timestamp, task_id):
        snap_url = f"https://web.archive.org/web/{task_id}"
        self.get_website(company, policy, timestamp, snap_url)


    def get_website(self, company, policy, timestamp, url):
        blob_name = f"{Stage.SNAP.value}/{company}/{policy}/{timestamp}.html"

        if self.storage.check_blob(blob_name):
            # Don't try-cach this because want to fail fast if blob service is out.
            logger.info(f"Blob {blob_name} exists. Skipping.")
        else:
            logger.debug(f"Requesting html for {url}")
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept-Charset': 'utf-8, iso-8859-1;q=0.5'
            }
            resp = self.http_client.get(url, timeout=90, headers=headers)
            try:
                resp.raise_for_status()
            except HTTPError as e:
                # Try without these headers for kicks. Sometimes works (meta.com)
                resp = self.http_client.get(url, timeout=90)
                resp.raise_for_status()

            logger.debug(f"Testing html encoding.")
            html_content, detected_encoding = self.decode_html(resp)

            # Extract and clean the HTML with encoding info
            logger.debug("Cleaning html.")
            cleaned_html = self.extract_main_text(html_content, encoding=detected_encoding or None)

            self.storage.upload_html_blob(cleaned_html, blob_name)
            logger.info(f"Saved snapshot to blob: {blob_name}")
