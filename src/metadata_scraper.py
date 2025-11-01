import logging
import json
import requests
from src.blob_utils import (ensure_container, check_blob, upload_json_blob)
from src.log_utils import setup_logger
from src.scraper_utils import sanitize_urlpath, load_urls

logger = setup_logger(__name__, logging.INFO)

def scrape_wayback_metadata(url):
    api_url = f"http://web.archive.org/cdx/search/cdx"
    params = {
        'url': url,
        'output': 'json'
    }

    try:
        response = requests.get(api_url, params=params, timeout=60)
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to get metadata for {url}: {e}")
        raise e

    try:
        data = response.json()
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON response for {url}: {e}")
        raise e

    return data


def get_wayback_metadata(url, company, output_container_name):  
    url_path = sanitize_urlpath(url)
    blob_name = f"wayback-snapshots/{company}/{url_path}/metadata.json"
    
    if check_blob('documents', blob_name):
        logger.debug(f"Using cached wayback metadata from {blob_name}")
    else:
        data = scrape_wayback_metadata(url)    
        upload_json_blob(json.dumps(data), output_container_name, blob_name)


def get_wayback_metadatas(input_container_name="documents", input_blob_name="static_urls.json", output_container_name="documents"):
    """
    Load URLs from blob storage and process each one
    """
    ensure_container(output_container_name)

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
                    raise e
        
        logger.info(f"Completed {company}")
    
    logger.info(f"Total processing complete: {total_processed} total.")
        




    


