import logging
import json
import requests
from src.blob_utils import (check_blob, upload_json_blob, load_json_blob)
from src.log_utils import setup_logger
from src.scraper_utils import sanitize_urlpath
from src.stages import Stage
import pandas as pd

logger = setup_logger(__name__, logging.INFO)

def scrape_wayback_metadata(url, company) -> dict:
    policy = sanitize_urlpath(url)
    blob_name = f"{Stage.META.value}/{company}/{policy}/metadata.json"
    
    if check_blob(blob_name, touch=True):
        logger.debug(f"Using cached wayback metadata from {blob_name}")
        return
    
    api_url = f"http://web.archive.org/cdx/search/cdx"
    params = {
        'url': url,
        'output': 'json'
    }
    
    try:
        response = requests.get(api_url, params=params, timeout=90)
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


def parse_wayback_metadata(company, policy) -> list[dict]:
    input_blob_name = f"{Stage.META.value}/{company}/{policy}/metadata.json"
    output_blob_name = f"{Stage.META.value}/{company}/{policy}/manifest.json"
    
    logger.debug("Loading snap metadata from: %s", input_blob_name)
    data = load_json_blob(input_blob_name)
        
    if len(data) <= 1:
        logger.info(f"Found 0 snapshots for {input_blob_name}")
        return []
    
    # First row is headers, rest are snapshots
    headers = data[0]
    snapshots = data[1:]
    snapshots = [dict(zip(headers, snapshot)) for snapshot in snapshots]
    snapshots = pd.DataFrame(snapshots)

    # Snaps without timestamps are invalid for our purposes.
    mask = snapshots['timestamp'].notna() & (snapshots['timestamp']!='')
    snapshots = snapshots.loc[mask]

    # Snaps that 403'd are invalid for our purposes
    mask = snapshots['statuscode'].notna() & snapshots['statuscode'].str.isnumeric() & (snapshots['statuscode'] < '400')
    snapshots = snapshots.loc[mask]
    
    logger.info(f"Found {len(snapshots)} valid snapshots for {input_blob_name}")
    snapshots = snapshots.to_dict('records')
    
    if len(snapshots) > 0:
        upload_json_blob(json.dumps(snapshots, indent=2), output_blob_name)

    return snapshots
    
def sample_wayback_metadata(metadata: list[dict], company, policy) -> list[dict]:
    # For testing, take an evenly spaced sample of snaps
    N = 10
    rfc3339 = "%Y%m%d%H%M%S"
    snapshots = pd.DataFrame.from_records(metadata)
    try:
        snapshots['datetime'] = pd.to_datetime(snapshots['timestamp'], format=rfc3339)
        bins = pd.cut(snapshots['datetime'], bins=min(N, len(snapshots)))
        snapshots['timebin'] = bins
        sample = snapshots.groupby('timebin', observed=True).first()
    except Exception as e:
        logger.error(f"Failed to sample snapshots for {company}/{policy}:\n{e}")
        # Fallback: take first N snapshots
        sample = snapshots.head(N)
    return sample.to_dict('records')