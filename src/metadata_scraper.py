import logging
import json
import requests
import time
from src.blob_utils import (check_blob, upload_json_blob)
from src.log_utils import setup_logger
from src.scraper_utils import sanitize_urlpath
from src.stages import Stage

logger = setup_logger(__name__, logging.INFO)

def scrape_wayback_metadata(url, company) -> dict:
    policy = sanitize_urlpath(url)
    blob_name = f"{Stage.META.value}/{company}/{policy}/metadata.json"
    
    if check_blob(blob_name):
        logger.debug(f"Using cached wayback metadata from {blob_name}")
        return
    
    api_url = f"http://web.archive.org/cdx/search/cdx"
    params = {
        'url': url,
        'output': 'json'
    }
    
    try:
        response = requests.get(api_url, params=params, timeout=60)
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Metadata request failed for {url}:\n{e}")
        raise
        
    try:
        data = response.json()
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON response for {url}:\n{e}")
        raise
    
    upload_json_blob(json.dumps(data), blob_name)
