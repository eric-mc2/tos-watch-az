import logging
import requests
import pandas as pd
import time
from bs4 import BeautifulSoup
from src.blob_utils import (check_blob, load_json_blob, upload_html_blob)
import chardet  # Add this import for encoding detection
from src.log_utils import setup_logger
from src.blob_utils import parse_blob_path
from src.stages import Stage

logger = setup_logger(__name__, logging.INFO)


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


def parse_wayback_metadata(blob_name):
    logger.debug("Loading snap metadata from: %s", blob_name)
    data = load_json_blob(blob_name)
        
    if len(data) <= 1:
        logger.info(f"Found 0 snapshots for {blob_name}")
        return None
    
    # First row is headers, rest are snapshots
    headers = data[0]
    snapshots = data[1:]
    snapshots = [dict(zip(headers, snapshot)) for snapshot in snapshots]
    snapshots = pd.DataFrame(snapshots)

    # Snaps without timestamps are invalid for our purposes.
    mask = snapshots['timestamp'].notna() & (snapshots['timestamp']!='')
    snapshots = snapshots.loc[mask]
    logger.info(f"Found {len(snapshots)} valid snapshots for {blob_name}")

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
        logger.error(f"Failed to sample snapshots for {blob_name}: {e}")
        # Fallback: take first N snapshots
        sample = snapshots.head(N)
    return sample


def scrape_wayback_snapshot(snap_url, blob_name, retries=2):
    try:
        # Add headers to mimic a real browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Charset': 'utf-8, iso-8859-1;q=0.5'
        }
        resp = requests.get(snap_url, timeout=30, headers=headers)
        resp.raise_for_status()
        return resp
    except Exception as e:
        logger.error(f"Error scraping URL: {snap_url}")
        if retries: 
            time.sleep(2)
            logger.warning(f"Retrying: {snap_url}")
            return scrape_wayback_snapshot(snap_url, blob_name, retries-1)
        else:
            raise


def get_wayback_snapshot(company, policy, timestamp, original_url):
    
    snap_url = f"https://web.archive.org/web/{timestamp}/{original_url}"
    blob_name = f"{Stage.SNAP.value}/{company}/{policy}/{timestamp}.html"
    
    if check_blob(blob_name):
        # Don't try-cach this because want to fail fast if blob service is out.
        logger.info(f"Blob {blob_name} exists. Skipping.")
    else:
        logger.debug(f"Requesting wayback html for {original_url}/{timestamp}")
        resp = scrape_wayback_snapshot(snap_url, blob_name)
        
        logger.debug(f"Testing html encoding.")
        html_content, detected_encoding = decode_html(resp)
                    
        # Extract and clean the HTML with encoding info
        logger.debug("Cleaning html.")
        cleaned_html = extract_main_text(html_content, encoding=detected_encoding or None)
        
        upload_html_blob(cleaned_html, blob_name)
        logger.info(f"Saved snapshot to blob: {blob_name}")


def get_wayback_snapshots(meta_blob_name):
    """
    Get wayback snapshots for a single URL and save to blob storage
    """
    data = parse_wayback_metadata(meta_blob_name)
    if data is None:
        return

    parsed_path = parse_blob_path(meta_blob_name)
    company = parsed_path[1]
    policy = parsed_path[2]

    # Track success/failure for this URL
    snapshots_saved = 0
    retries = 2
    
    # Now actually download the snap html and save to blob storage
    for timestamp, original_url in zip(data['timestamp'], data['original']):
        try:
            get_wayback_snapshot(company, policy, timestamp, original_url)
            snapshots_saved += 1
        except Exception as e:
            logger.error(f"Failed to scrape SNAP {timestamp} of URL {original_url}: {e}")
            if retries:
                retries -= 1
            else:
                raise
            

    logger.info(f"URL {meta_blob_name} complete: {snapshots_saved} saved")
