import logging
import json
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
import azure.functions as func
from shared.blob_utils import (ensure_container, check_blob, load_json_blob, upload_json_blob, upload_html_blob)
from azure.storage.blob import ContentSettings
import chardet  # Add this import for encoding detection
from urllib.error import HTTPError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

az_logger = logging.getLogger('azure.*')
az_logger.setLevel(logging.WARNING)

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

def validate_url(url):
    """Validate URL format"""
    from urllib.parse import urlparse
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False

def sanitize_path_component(path_component):
    """Sanitize a path component for use in blob names"""
    import re
    # Replace invalid characters with underscores
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', path_component)
    # Remove any leading/trailing whitespace and dots
    sanitized = sanitized.strip(' .')
    # Ensure it's not empty
    return sanitized if sanitized else 'default'

def load_urls(input_container_name, input_blob_name):
     # Download the URLs file from blob storage
     # Validate URL
    urls = load_json_blob(input_container_name, input_blob_name)
    for company, url_list in urls.items():
        for url in url_list:
            if not validate_url(url):
                raise ValueError(f"Invalid URL format: {url}")
    return urls

def get_wayback_snapshots(input_container_name="documents", input_blob_name="static_urls.json", output_container_name="documents"):
    """
    Load URLs from blob storage and process each one
    """
    ensure_container(output_container_name)

    urls = load_urls(input_container_name, input_blob_name)
    logging.info(f"Found {len(urls)} companies with URLs to process")
    
    # Process each URL grouping
    company_processed = 0
    total_processed = 0
    company_pending = len(urls)
    retries = 2
    
    for company, url_list in urls.items():
        logging.info(f"Processing {len(url_list)} URLs for {company}")
        
        for url in url_list:
            try:
                get_wayback_snapshot(url, company, output_container_name)
                total_processed += 1
            except Exception as e:
                logging.error(f"Failed to process URL {url} for {company}: {e}")
                if retries:
                    retries -= 1
                else:
                    raise e
        
        company_pending -= 1
        logging.info(f"Completed {company}: {company_processed} processed, {company_pending} pending")
    
    logging.info(f"Total processing complete: {company_processed} companies, {total_processed} total.")
        
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
        logging.error(f"Failed to get metadata for {url}: {e}")
        raise e

    try:
        data = response.json()
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse JSON response for {url}: {e}")
        raise e

    return data
    
def get_wayback_metadata(url, company):  
    url_path = sanitize_urlpath(url)
    blob_name = f"wayback-snapshots/{company}/{url_path}/metadata.json"
    
    if check_blob('documents', blob_name):
        logging.debug(f"Using cached wayback metadata from {blob_name}")
        data = load_json_blob('documents', blob_name)
    else:
        data = scrape_wayback_metadata(url)    
        upload_json_blob(data, 'documents', blob_name) 
    
    if len(data) <= 1:
        logging.info(f"Found 0 snapshots for {url}")
        return None
    
    # First row is headers, rest are snapshots
    headers = data[0]
    snapshots = data[1:]
    snapshots = [dict(zip(headers, snapshot)) for snapshot in snapshots]
    snapshots = pd.DataFrame(snapshots)

    # Snaps without timestamps are invalid for our purposes.
    mask = snapshots['timestamp'].notna() & (snapshots['timestamp']!='')
    snapshots = snapshots.loc[mask]
    logging.info(f"Found {len(snapshots)} valid snapshots for {url}")

    if len(snapshots) == 0:
        return None
    
    # For testing, take an evenly spaced sample of snaps
    N = 10
    rfc3339 = "%Y%m%d%H%M%S"
    try:
        snapshots['datetime'] = pd.to_datetime(snapshots['timestamp'], format=rfc3339)
        bins = pd.cut(snapshots['datetime'], bins=min(N, len(snapshots)))
        snapshots['timebin'] = bins
        sample = snapshots.groupby('timebin', observed=True).first()
    except Exception as e:
        logging.error(f"Failed to sample snapshots for {url}: {e}")
        # Fallback: take first N snapshots
        sample = snapshots.head(N)
    return sample

def sanitize_urlpath(url):
    # Parse URL for file structure
    from urllib.parse import urlparse
    from pathlib import Path
    parsed_url = urlparse(url)
    url_path = parsed_url.path if parsed_url.path != '/' else parsed_url.netloc
    url_path = Path(url_path).parts[-1] or 'index'
    url_path = sanitize_path_component(url_path)
    return url_path

def get_wayback_snapshot(url, company, output_container_name):
    """
    Get wayback snapshots for a single URL and save to blob storage
    """
    data = get_wayback_metadata(url, company)
    if data is None:
        return

    url_path = sanitize_urlpath(url)
    
    # Track success/failure for this URL
    snapshots_saved = 0
    retries = 1
    
    # Now actually download the snap html and save to blob storage
    for timestamp, original_url in zip(data['timestamp'], data['original']):
        snap_url = f"https://web.archive.org/web/{timestamp}/{original_url}"
        blob_name = f"wayback-snapshots/{company}/{url_path}/{timestamp}.html"

        try:
            if check_blob(output_container_name, blob_name):
                logging.info(f"Blob {output_container_name}/{blob_name} exists. Skipping.")
                continue
            
            # Add headers to mimic a real browser
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept-Charset': 'utf-8, iso-8859-1;q=0.5'
            }
            
            logging.debug(f"Requesting wayback html for {original_url}/{timestamp}")
            resp = requests.get(snap_url, timeout=30, headers=headers)
            resp.raise_for_status()

            logging.debug(f"Testing html encoding.")
            html_content, detected_encoding = decode_html(resp)
                        
            # Extract and clean the HTML with encoding info
            logging.debug("Cleaning html.")
            cleaned_html = extract_main_text(html_content, encoding=detected_encoding or None)
            
            logging.debug("Uploading blob: {company}/{original_url}/{timestamp}")
            upload_html_blob(cleaned_html, output_container_name, blob_name)
                        
            snapshots_saved += 1
            logging.info(f"Saved snapshot to blob: {output_container_name}/{blob_name}")
        except Exception as e:
            if retries: 
                retries -= 1
            else:
                raise e
            
    logging.info(f"URL {url} complete: {snapshots_saved} saved")
            
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Starting wayback snapshot collection process.')

    try:
        get_wayback_snapshots()        
        return func.HttpResponse(
            f"Successfully processed wayback snapshots",
            status_code=200
        )
        
    except Exception as e:
        logging.error(f"Error processing wayback snapshots: {e}")
        return func.HttpResponse(
            f"Error processing wayback snapshots: {str(e)}",
            status_code=500
        )
