import logging
import json
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
import azure.functions as func
from shared.blob_utils import get_blob_service_client, ensure_container
import chardet  # Add this import for encoding detection

# Configure logging at module level for Azure Functions
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def extract_main_text(html_content, encoding='utf-8'):
    """Extract main content from HTML with proper encoding handling"""
    # Ensure we have properly decoded content
    if isinstance(html_content, bytes):
        # Try to detect encoding if not provided
        detected = chardet.detect(html_content)
        detected_encoding = detected.get('encoding', 'utf-8') if detected else 'utf-8'
        
        # Try the detected encoding first, fallback to utf-8
        try:
            html_content = html_content.decode(detected_encoding)
        except (UnicodeDecodeError, LookupError):
            try:
                html_content = html_content.decode('utf-8', errors='replace')
            except:
                html_content = html_content.decode('latin1', errors='replace')
    
    # Parse with BeautifulSoup, explicitly handling encoding
    soup = BeautifulSoup(html_content, "html.parser", from_encoding=encoding)
    
    # Try to find the main content; fallback to body text
    main = soup.find('main')
    if main:
        return str(main)
    # Remove scripts, styles, footers, sidebars, and ads
    for tag in soup(['script', 'style', 'footer', 'aside', 'nav']):
        tag.decompose()
    # Optionally remove common ad containers
    for ad_tag in soup.find_all(class_=['ad', 'ads', 'advertisement']):
        ad_tag.decompose()
    body = soup.body
    return str(body) if body else str(soup)

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

def get_wayback_snapshots(input_container_name="documents", input_blob_name="static_urls.json", output_container_name="documents/wayback-snapshots"):
    """
    Load URLs from blob storage and process each one
    """
    blob_service_client = get_blob_service_client()
    
    ensure_container(blob_service_client, output_container_name)
    
    # Download the URLs file from blob storage
    try:
        blob_client = blob_service_client.get_blob_client(container=input_container_name, blob=input_blob_name)
        urls_data = blob_client.download_blob().readall()
        urls = json.loads(urls_data.decode('utf-8'))
        logging.info(f"Loaded URLs from blob storage: {input_container_name}/{input_blob_name}")
        logging.info(f"Found {len(urls)} companies with URLs to process")
    except Exception as e:
        logging.error(f"Failed to load URLs from blob storage: {e}")
        raise
    
    # Process each URL grouping
    total_processed = 0
    total_failed = 0
    
    for company, url_list in urls.items():
        if not isinstance(url_list, list):
            logging.warning(f"Skipping {company}: URLs should be a list, got {type(url_list)}")
            continue
            
        logging.info(f"Processing {len(url_list)} URLs for {company}")
        company_processed = 0
        company_failed = 0
        
        for url in url_list:
            try:
                get_wayback_snapshot(url, company, output_container_name)
                company_processed += 1
                total_processed += 1
            except Exception as e:
                logging.error(f"Failed to process URL {url} for {company}: {e}")
                company_failed += 1
                total_failed += 1
                continue
        
        logging.info(f"Completed {company}: {company_processed} processed, {company_failed} failed")
    
    logging.info(f"Total processing complete: {total_processed} processed, {total_failed} failed")
        
def get_wayback_snapshot(url, company, output_container_name):
    """
    Get wayback snapshots for a single URL and save to blob storage
    """
    # Validate URL
    if not validate_url(url):
        logging.error(f"Invalid URL format: {url}")
        return
    
    blob_service_client = get_blob_service_client()
    
    api_url = f"http://web.archive.org/cdx/search/cdx"
    params = {
        'url': url,
        'output': 'json'
    }

    try:
        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Failed to get snapshots for {url}: {e}")
        return

    try:
        data = response.json()
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse JSON response for {url}: {e}")
        return

    if len(data) <= 1:
        logging.info(f"Found 0 snapshots for {url}")
        return

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
        return

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

    # Parse URL for file structure
    from urllib.parse import urlparse
    from pathlib import Path
    parsed_url = urlparse(url)
    url_path = parsed_url.path if parsed_url.path != '/' else parsed_url.netloc
    url_path = Path(url_path).parts[-1] or 'index'
    url_path = sanitize_path_component(url_path)
    
    # Track success/failure for this URL
    snapshots_saved = 0
    snapshots_failed = 0
    
    # Now actually download the snap html and save to blob storage
    for idx, row in sample.iterrows():
        try:
            timestamp = row['timestamp']
            original_url = row['original']
            
            snap_url = f"https://web.archive.org/web/{timestamp}/{original_url}"
            
            # Add headers to mimic a real browser
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept-Charset': 'utf-8, iso-8859-1;q=0.5'
            }
            
            resp = requests.get(snap_url, timeout=30, headers=headers)
            resp.raise_for_status()
            
            # Handle encoding properly
            # First, try to detect the actual encoding from the response
            detected_encoding = None
            if resp.headers.get('content-type'):
                content_type = resp.headers['content-type'].lower()
                if 'charset=' in content_type:
                    detected_encoding = content_type.split('charset=')[1].split(';')[0].strip()
            
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
                html_content = resp.content.decode('utf-8', errors='replace')
            
            # Extract and clean the HTML with encoding info
            txt = extract_main_text(html_content, encoding=detected_encoding or 'utf-8')
            
            # Parse again to ensure clean, properly encoded output
            soup = BeautifulSoup(txt, "html.parser")
            cleaned_html = soup.prettify()
            
            # Create blob path: company/url_path/timestamp.html
            blob_name = f"{company}/{url_path}/{timestamp}.html"
            
            # Upload to blob storage with explicit UTF-8 encoding
            blob_client = blob_service_client.get_blob_client(
                container=output_container_name, 
                blob=blob_name
            )
            # Ensure we upload as UTF-8 bytes
            html_bytes = cleaned_html.encode('utf-8')
            blob_client.upload_blob(
                html_bytes, 
                overwrite=True,
                content_settings={'content_type': 'text/html; charset=utf-8'}
            )
            
            snapshots_saved += 1
            logging.info(f"Saved snapshot to blob: {output_container_name}/{blob_name}")
            
        except Exception as e:
            snapshots_failed += 1
            logging.error(f"Failed to process snapshot {timestamp} for {url}: {e}")
            continue
    
    logging.info(f"URL {url} complete: {snapshots_saved} saved, {snapshots_failed} failed")
            
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Starting wayback snapshot collection process.')

    try:
        # Process the wayback snapshots
        get_wayback_snapshots()
        
        return func.HttpResponse(
            f"Successfully processed wayback snapshots", #. Input: {input_container}/{input_blob}, Output: {output_container}",
            status_code=200
        )
        
    except Exception as e:
        logging.error(f"Error processing wayback snapshots: {e}")
        return func.HttpResponse(
            f"Error processing wayback snapshots: {str(e)}",
            status_code=500
        )
