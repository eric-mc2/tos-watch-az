import logging
import json
import requests
import time
from src.blob_utils import (check_blob, upload_json_blob)
from src.log_utils import setup_logger
from src.scraper_utils import sanitize_urlpath, load_urls
from src.stages import Stage

logger = setup_logger(__name__, logging.INFO)

def scrape_wayback_metadata(url, retries=2):
    api_url = f"http://web.archive.org/cdx/search/cdx"
    params = {
        'url': url,
        'output': 'json'
    }
    try:
        response = requests.get(api_url, params=params, timeout=60)
        response.raise_for_status()
        return response
    except Exception as e:
        logger.error(f"Failed to get metadata for {url}: {e}")
        if retries:
            time.sleep(2) # wait politely
            logger.warning(f"Retrying: {url}")
            return scrape_wayback_metadata(url, retries - 1)
        else:
            raise

def get_wayback_metadata(url, company, output_container_name):
    url_path = sanitize_urlpath(url)
    blob_name = f"{Stage.SNAP.value}/{company}/{url_path}/metadata.json"
    
    if check_blob('documents', blob_name):
        logger.debug(f"Using cached wayback metadata from {blob_name}")
    else:
        resp = scrape_wayback_metadata(url)    
        try:
            data = resp.json()
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response for {url}: {e}")
            raise
        upload_json_blob(json.dumps(data), output_container_name, blob_name)


def get_wayback_metadatas(input_container_name="documents", input_blob_name="static_urls.json", output_container_name="documents"):
    """
    Load URLs from blob storage and process each one
    """
    urls = load_urls(input_container_name, input_blob_name)
    logger.info(f"Found {len(urls)} companies with URLs to process")
    
    # Process each URL grouping
    total_processed = 0
    retries = 2
    
    for company, url_list in urls.items():
        logger.info(f"Processing {len(url_list)} URLs for {company}")
        
        for url in url_list:
            try:
                get_wayback_metadata(url, company, output_container_name)
                total_processed += 1
            except Exception as e:
                logger.error(f"Failed to process URL {url} for {company}: {e}")
                if retries:
                    retries -= 1
                else:
                    raise
        
        logger.info(f"Completed {company}")
    
    logger.info(f"Total processing complete: {total_processed} total.")
        




    


