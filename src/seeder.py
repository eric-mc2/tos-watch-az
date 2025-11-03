import logging
from src.blob_utils import upload_json_blob
from src.log_utils import setup_logger
from src.scraper_utils import validate_url, sanitize_urlpath
import json

logger = setup_logger(__name__, logging.INFO)


def validate_urls(urls: dict):
    for company, urls in urls.items():
        for url in urls:
            if not validate_url(url):
                raise ValueError(f"Invalid url: {url}")
            fp = sanitize_urlpath(url)
            if not fp:
                raise ValueError(f"Invalid url -> filename: {url} -> {fp}")


def process_urls(urls: dict):
    # Note: this is separate to make the validate/upload logic testable
    #       and separate from the hard-coded data which is hard to inject.
    validate_urls(urls)
    text_content = json.dumps(urls, indent=2)
    upload_json_blob(text_content, 'static_urls.json')


def seed_urls() -> None:
    with open('data/static_urls.json') as f:
        urls = f.read()
    urls = json.loads(urls)
    process_urls(urls)