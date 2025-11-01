import logging
from src.blob_utils import ensure_container, upload_json_blob
from src.log_utils import setup_logger

logger = setup_logger(__name__, logging.INFO)
            
def main() -> None:
    with open('data/static_urls.json') as f:
        urls = f.read()
    ensure_container('documents')
    upload_json_blob(urls, 'documents', 'static_urls.json')
