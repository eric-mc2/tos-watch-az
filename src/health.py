from azure import functions as func
import requests
import json
import logging
from src.blob_utils import list_blobs
from src.scraper_utils import load_urls, sanitize_urlpath
from src.metadata_scraper import parse_wayback_metadata
from src.stages import Stage
from src.log_utils import setup_logger

logger = setup_logger(__name__, logging.DEBUG)

def list_in_flight(workflow_type: str = None, runtimes: str|list[str] = None) -> str:
    params = {}
    if runtimes is not None:
        params["runtimeStatus"] = runtimes
    
    data = _list_in_flight_paged(params)

    data = [dict(
            name = t.get('name'),
            runtime_status = t.get('runtimeStatus'),
            created = t.get('createdTime'),
            updated = t.get('lastUpdatedTime'),
            custom_status = t.get("customStatus"),
            input_data = t.get('input'))
            for t in data]
    
    for d in data:
        in_data = d['input_data']
        if isinstance(in_data, str):
            d['input_data'] = json.loads(in_data)
    
    if workflow_type is not None:
        data = [t for t in data if t['input_data']['workflow_type'] == workflow_type]

    return data


def _list_in_flight_paged(params, pages = None, token=None):
    if pages is None:
        pages = []
    headers = {"x-ms-continuation-token": token} if token is not None else None
    resp = requests.get("http://127.0.0.1:7071/runtime/webhooks/durabletask/instances", 
                        params=params,
                        headers=headers)
    resp.raise_for_status()
    pages.extend(resp.json())
    next_page = resp.headers.get("x-ms-continuation-token")
    if next_page:
        return _list_in_flight_paged(params, pages, token=next_page)
    else:
        return pages
    

def validate_exists() -> func.HttpResponse:
    blobs = set(list_blobs())
    if "static_urls.json" not in blobs:
        return func.HttpResponse("URLs blob missing")
    urls = load_urls("documents/static_urls.json")
    missing_metadata = []
    missing_snaps = []
    meta_counter, snap_counter = 0, 0
    for company, url_list in urls.items():
        for url in url_list:
            meta_counter += 1
            policy = sanitize_urlpath(url)
            blob_name = f"{Stage.META.value}/{company}/{policy}/metadata.json"
            if blob_name not in blobs:
                missing_metadata.append(blob_name)
                continue
            meta = parse_wayback_metadata(blob_name)
            for timestamp in meta['timestamp']:
                snap_counter += 1
                blob_name = f"{Stage.SNAP.value}/{company}/{policy}/{timestamp}.html"
                if blob_name not in blobs:
                    missing_snaps.append(blob_name)
    return func.HttpResponse("Missing Metadata {}/{}: {}\n\nMissing Snapshots {}/{}: {}".format(
        len(missing_metadata), meta_counter, json.dumps(missing_metadata, indent=2), 
        len(missing_snaps), snap_counter, json.dumps(missing_snaps, indent=2)
    ))