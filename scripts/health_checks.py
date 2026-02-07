from typing import Optional

import requests
import json
import logging
import os
import argparse
from collections import Counter
from azure import durable_functions as df  # type: ignore

from src.adapters.http.client import RequestsAdapter
from src.adapters.llm.client import ClaudeAdapter
from src.adapters.storage.client import AzureStorageAdapter
from src.container import ServiceContainer
from src.orchestration.orchestrator import WORKFLOW_CONFIGS
from src.services.blob import BlobService
from src.services.llm import LLMService
from src.utils.app_utils import load_env_vars
from src.utils.log_utils import setup_logger
from src.utils.path_utils import extract_policy
from src.stages import Stage
from src.transforms.seeds import STATIC_URLS

# TODO: refactor to properly used DI services and dev/stage/prod

setup_logger(__name__, logging.WARNING)
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

load_env_vars()

KILL_CIRCUIT = "KILL_CIRCUIT"
KILL_ALL = "KILL_ALL"

def validate_files(env, *args, **kwargs) -> dict:
    # TODO: Consolidate environment handling into service container?
    conn_key = "APP_BLOB_CONNECTION_STRING" if env == "PROD" else "AzureWebJobsStorage"
    storage = BlobService(AzureStorageAdapter(conn_key))
    http = RequestsAdapter()
    llm = LLMService(ClaudeAdapter())
    container = ServiceContainer.create_container(storage, http, llm)
    try:
        blobs = set(container.storage.adapter.list_blobs())
    except RuntimeError as e:
        return {"error": str(e)}
    urls = STATIC_URLS
    missing_metadata: list[str] = []
    missing_snaps: list[str] = []
    missing_docs: list[str] = []
    missing_trees: list[str] = []
    missing_diff: list[str] = []
    meta_counter, snap_counter = 0, 0
    for company, url_list in urls.items():
        for url in url_list:
            meta_counter += 1
            policy = extract_policy(url)
            blob_name = f"{Stage.META.value}/{company}/{policy}/manifest.json"
            if blob_name not in blobs:
                missing_metadata.append(blob_name)
                continue
            metadata = container.storage.load_json_blob(blob_name)
            assert isinstance(metadata, list)
            meta = container.wayback_transform.sample_wayback_metadata(metadata, company, policy)
            for row in meta:
                timestamp = row['timestamp']
                snap_counter += 1
                blob_name = f"{Stage.SNAP.value}/{company}/{policy}/{timestamp}.html"
                if blob_name not in blobs:
                    missing_snaps.append(blob_name)
                blob_name = f"{Stage.DOCTREE.value}/{company}/{policy}/{timestamp}.json"
                if blob_name not in blobs:
                    missing_trees.append(blob_name)
                blob_name = f"{Stage.DOCCHUNK.value}/{company}/{policy}/{timestamp}.json"
                if blob_name not in blobs:
                    missing_docs.append(blob_name)
                blob_name = f"{Stage.DIFF_RAW.value}/{company}/{policy}/{timestamp}.json"
                if blob_name not in blobs:
                    missing_diff.append(blob_name)
    return {"Missing Metadata Count": f"{len(missing_metadata)}/{meta_counter}",
            "Missing Metadata Files":  missing_metadata,
            "Missing Snapshot Count": f"{len(missing_snaps)}/{snap_counter}",
             "Missing Snapshot Files": missing_snaps,
            "Missing Trees Count": f"{len(missing_trees)}/{snap_counter}",
             "Missing Trees Files": missing_trees,
            "Missing Docs Count": f"{len(missing_docs)}/{snap_counter}",
             "Missing Docs Files": missing_docs,
            "Missing Diffs Count": f"{len(missing_diff)}/{snap_counter}",
             "Missing Diffs Files": missing_diff,
     }


def kill_all(env: str, workflow_type: str, reason: str = KILL_CIRCUIT):
    """
    Terminate all running orchestrations.
    
    Args:
        workflow_type: Optional workflow type to filter which orchestrations to terminate
        reason: Reason for termination (default: "Manual termination")
    
    Returns:
        dict with count of terminated orchestrations and their details
    """
    # Get all running/pending orchestrations
    in_flight = list_in_flight(
        env=env,
        workflow_type=workflow_type,
        runtimes="Running", #["Running", "Pending", "Suspended", "ContinuedAsNew"]
    )
    
    if in_flight['count'] == 0:
        return {"count": 0, "terminated": [], "message": "No orchestrations to terminate"}
    
    terminated = []
    for task in in_flight['tasks']:
        instance_id = task.get('instance_id')
        should_terminate = instance_id is not None and task['name'] == "orchestrator"
        if reason == KILL_CIRCUIT:
            should_terminate &= task['custom_status'] == "Waiting for circuit"
        elif reason == KILL_ALL:
            should_terminate &= True
        else:
            should_terminate = False
        if should_terminate:
            try:
                # Use REST API to terminate
                url = f"{_get_app_url(env)}/runtime/webhooks/durabletask/instances/{instance_id}/terminate"
                params = {
                    'reason': reason,
                    'code': os.environ.get("AZURE_FUNCTION_MASTER_KEY")
                }
                resp = requests.post(url, params=params)
                resp.raise_for_status()
                
                terminated.append({
                    "instance_id": instance_id,
                    "task_id": task.get('input_data', {}).get('task_id'),
                    "updated": task['updated']
                })
            except Exception as e:
                logging.error(f"Failed to terminate {instance_id}: {type(e)} {e}")
    
    return {
        "count": len(terminated),
        "terminated": terminated,
        "reason": reason
    }


def list_in_flight(env: str, workflow_type: Optional[str] = None, runtimes: Optional[str|list[str]] = None) -> dict:
    params = {}

    if runtimes is None:
        runtimes = ["Running", "Pending", "Suspended", "ContinuedAsNew"]
    params["runtimeStatus"] = runtimes

    params['code'] = str(os.environ.get("AZURE_FUNCTION_MASTER_KEY"))
   
    data = _list_in_flight_paged(params, env)

    data = [dict(
            name = t.get('name'),
            runtime_status = t.get('runtimeStatus'),
            created = t.get('createdTime'),
            updated = t.get('lastUpdatedTime'),
            custom_status = t.get("customStatus"),
            instance_id = t.get('instanceId'),
            input_data = t.get('input'))
            for t in data]
    
    for d in data:
        in_data = d['input_data']
        if isinstance(in_data, str):
            d['input_data'] = json.loads(in_data)
    
    filtered_data = []
    for d in data:
        if workflow_type is None or d.get('input_data', {}).get('workflow_type') == workflow_type:
            filtered_data.append(d)

    names = Counter([t['name'] for t in filtered_data])
    statuses = Counter([t['runtime_status'] for t in filtered_data])
    # TODO: Add waiting for circuit reporting
    throttled = Counter([t['custom_status'] is not None and 'Throttled' in t.get('custom_status', '') for t in filtered_data])
    workflows = Counter([t['input_data'].get('workflow_type') for t in filtered_data])
    companies = Counter([t['input_data'].get('company') for t in filtered_data])

    formatted = dict(
        count = len(filtered_data),
        tasks = filtered_data,
        summary = dict(names = names,
                       statuses = statuses,
                       throttled = throttled,
                       workflows = workflows,
                       companies = companies),
    )

    return formatted


def _list_in_flight_paged(params, env, pages = None, next_token=None):
    if pages is None:
        pages = []
    
    headers = {}
    if next_token is not None:
        headers["x-ms-continuation-token"] = next_token

    resp = requests.get(_get_app_url(env) + "/runtime/webhooks/durabletask/instances", 
                        params=params,
                        headers=headers)
    resp.raise_for_status()
    pages.extend(resp.json())
    next_page = resp.headers.get("x-ms-continuation-token")
    if next_page:
        return _list_in_flight_paged(params, env, pages, next_token=next_page)
    else:
        return pages


def _get_app_url(env):
    if env == "PROD":
        app_url = os.environ.get('WEBSITE_HOSTNAME')
        if not app_url:
            raise RuntimeError("Environment variable WEBSITE_HOSTNAME not set.")
    else:
        app_url = "http://127.0.0.1:7071"
    return app_url


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                    prog='health_checks',
                    description='List in flight tasks (run az login before)')
    subparsers = parser.add_subparsers(required=True)

    parser_tasks = subparsers.add_parser('tasks', help='list running tasks')
    parser_tasks.add_argument("--workflow_type")
    parser_tasks.add_argument("--output")
    parser_tasks.add_argument("--env", choices=["DEV","PROD"])
    parser_tasks.add_argument("--runtimes", action='append', default=None, choices=df.OrchestrationRuntimeStatus._member_names_)
    parser_tasks.set_defaults(func=list_in_flight)

    parser_files = subparsers.add_parser('files', help='list missing files')
    parser_files.add_argument("--output")
    parser_files.add_argument("--env", choices=["DEV","PROD"])
    parser_files.set_defaults(func=validate_files)

    parser_kill = subparsers.add_parser('kill', help='terminate all running orchestrations')
    parser_kill.add_argument("--workflow_type", required=True, choices=WORKFLOW_CONFIGS, help='only terminate specific workflow type')
    parser_kill.add_argument("--output")
    parser_kill.add_argument("--env", choices=["DEV","PROD"])
    parser_kill.add_argument("--reason", default=KILL_CIRCUIT, help='termination reason')
    parser_kill.set_defaults(func=kill_all)

    args = parser.parse_args()
    
    # Extract function arguments
    func_kwargs = {k: v for k, v in vars(args).items() if k not in ['func', 'output']}
    output = args.func(**func_kwargs)

    if args.output:
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
    else:
        print(json.dumps(output, indent=2))