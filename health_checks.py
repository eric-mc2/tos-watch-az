import requests
import json
import logging
import os
import argparse
from dotenv import load_dotenv
from azure import functions as func
from azure import durable_functions as df
from src.orchestrator import WORKFLOW_CONFIGS
from src.log_utils import setup_logger
from src.blob_utils import list_blobs
from src.scraper_utils import sanitize_urlpath
from src.metadata_scraper import parse_wayback_metadata
from src.stages import Stage
from src.seeder import STATIC_URLS

load_dotenv()
setup_logger(__name__, logging.WARNING)
logging.getLogger("azure").setLevel(logging.WARNING)


def validate_exists(*args, **kwargs) -> str:
    try:
        blobs = set(list_blobs())
    except RuntimeError as e:
        return str(e)
    if "static_urls.json" not in blobs:
        return "URLs blob missing"
    urls = STATIC_URLS
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
            meta = parse_wayback_metadata(company, policy)
            for row in meta:
                timestamp = row['timestamp']
                snap_counter += 1
                blob_name = f"{Stage.SNAP.value}/{company}/{policy}/{timestamp}.html"
                if blob_name not in blobs:
                    missing_snaps.append(blob_name)
    return "Missing Metadata {}/{}: {}\n\nMissing Snapshots {}/{}: {}".format(
        len(missing_metadata), meta_counter, json.dumps(missing_metadata, indent=2), 
        len(missing_snaps), snap_counter, json.dumps(missing_snaps, indent=2)
    )

def list_in_flight(workflow_type: str = None, runtimes: str|list[str] = None) -> dict:

    params = {}

    if runtimes is None:
        runtimes = ["Running", "Pending", "Suspended", "ContinuedAsNew"]
    params["runtimeStatus"] = runtimes

    params['code'] = os.environ.get("AZURE_FUNCTION_MASTER_KEY")
   
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

    formatted = dict(
        count = len(data),
        tasks = data
    )

    return formatted


def _list_in_flight_paged(params, pages = None, next_token=None):
    if pages is None:
        pages = []
    
    headers = {}
    if next_token is not None:
        headers["x-ms-continuation-token"] = next_token

    resp = requests.get(_get_app_url() + "/runtime/webhooks/durabletask/instances", 
                        params=params,
                        headers=headers)
    resp.raise_for_status()
    pages.extend(resp.json())
    next_page = resp.headers.get("x-ms-continuation-token")
    if next_page:
        return _list_in_flight_paged(params, headers, pages, next_token=next_page)
    else:
        return pages


def _get_app_url():
    app_url = os.environ.get('WEBSITE_HOSTNAME')
    if app_url:
        app_url = f"https://{app_url}"
    else:
        app_url = "http://127.0.0.1:7071"
    return app_url


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                    prog='health_checks',
                    description='List in flight tasks (run az login before)')
    parser.add_argument("--output")
    subparsers = parser.add_subparsers(required=True)
    
    parser_tasks = subparsers.add_parser('tasks', help='list running tasks')
    parser_tasks.add_argument("--workflow_type", choices=WORKFLOW_CONFIGS)
    parser_tasks.add_argument("--runtime", action='append', default=None, choices=df.OrchestrationRuntimeStatus._member_names_)
    parser_tasks.set_defaults(func=list_in_flight)
    
    parser_files = subparsers.add_parser('files', help='list missing files')
    parser_files.set_defaults(func=validate_exists)

    args = parser.parse_args()
    output = args.func(args)

    if args.output:
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
    else:
        print(json.dumps(output, indent=2))