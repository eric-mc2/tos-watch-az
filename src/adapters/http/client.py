import requests
from src.adapters.http.protocol import HttpProtocol
import logging
from src.utils.log_utils import setup_logger
from requests.exceptions import HTTPError
import time

logger = setup_logger(__name__, logging.INFO)


class RequestsAdapter(HttpProtocol):
    """Production HTTP adapter using requests library"""

    # Different User-Agent strings to try
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    ]

    @staticmethod
    def get_browser_headers(user_agent=None, referer=None):
        """Generate comprehensive browser-like headers"""
        if user_agent is None:
            user_agent = RequestsAdapter.USER_AGENTS[0]

        headers = {
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        }

        if referer:
            headers['Referer'] = referer

        return headers


    def get(self, url: str, **kwargs) -> requests.Response:
        logger.debug(f"Requesting html for {url}")

        # Try with full browser headers first
        headers = self.get_browser_headers()

        try:
            resp = requests.get(url, timeout=90, headers=headers)
            resp.raise_for_status()
            return resp
        except HTTPError as e:
            if e.response.status_code == 403:
                logger.warning(f"Got 403 with default headers, trying alternatives...")

                # Try different User-Agent strings
                for i, ua in enumerate(self.USER_AGENTS[1:], 1):
                    try:
                        logger.debug(f"Attempt {i + 1}: Trying with different User-Agent")
                        headers = self.get_browser_headers(user_agent=ua)
                        time.sleep(1)  # Small delay between attempts
                        resp = requests.get(url, timeout=90, headers=headers)
                        resp.raise_for_status()
                        logger.debug(f"Success with User-Agent attempt {i + 1}")
                        return resp
                    except HTTPError:
                        continue

                # Try with a referer (pretend we came from the site's homepage)
                try:
                    from urllib.parse import urlparse
                    parsed = urlparse(url)
                    referer = f"{parsed.scheme}://{parsed.netloc}"
                    logger.debug(f"Trying with Referer: {referer}")
                    headers = self.get_browser_headers(referer=referer)
                    time.sleep(1)
                    resp = requests.get(url, timeout=90, headers=headers)
                    resp.raise_for_status()
                    logger.info("Success with Referer header")
                    return resp
                except HTTPError:
                    pass

                # Last resort: minimal headers (sometimes works for meta.com)
                logger.debug("Trying with minimal headers as last resort")
                time.sleep(1)
                resp = requests.get(url, timeout=90)
                resp.raise_for_status()
                return resp
            else:
                raise

