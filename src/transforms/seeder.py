import logging
from dataclasses import dataclass

from src.utils.log_utils import setup_logger
from src.utils.path_utils import validate_url, extract_policy
import json

from src.services.blob import BlobService
from src.transforms.seeds import STATIC_URLS

logger = setup_logger(__name__, logging.INFO)

@dataclass
class Seeder:
    storage: BlobService

    @staticmethod
    def validate_urls(urls: dict):
        for company, urls in urls.items():
            for url in urls:
                if not validate_url(url):
                    raise ValueError(f"Invalid url: {url}")
                fp = extract_policy(url)
                if not fp:
                    raise ValueError(f"Invalid url -> filename: {url} -> {fp}")

    @staticmethod
    def sanitize_urls(urls: dict):
        return {url: f"{company}/{extract_policy(url)}" for company, urls in urls.items() for url in urls}

    def seed_urls(self, urls: dict = STATIC_URLS):
        self.validate_urls(urls)
        self.storage.upload_json_blob(json.dumps(urls, indent=2), 'static_urls.json')
        url_paths = self.sanitize_urls(urls)
        self.storage.upload_json_blob(json.dumps(url_paths, indent=2), 'url_blob_paths.json')