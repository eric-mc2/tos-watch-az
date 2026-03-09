from typing import Optional

import requests
import json
import logging
import os
import argparse
from collections import Counter
from azure import durable_functions as df  # type: ignore
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime

from src.transforms.icl import SummaryDataLoader
from src.container import ServiceContainer
from src.orchestration.orchestrator import WORKFLOW_CONFIGS
from src.services.blob import RunBlobPath
from src.utils.app_utils import load_env_vars
from src.utils.log_utils import setup_logger
from src.stages import Stage

setup_logger(__name__, logging.WARNING)

KILL_CIRCUIT = "KILL_CIRCUIT"
KILL_ALL = "KILL_ALL"

def validate_files() -> dict:
    container = ServiceContainer.create_real()
    blobs: dict[tuple, list] = {}
    metas: dict[tuple, list] = {}
    for blob in container.storage.adapter.list_blobs():
        parts = container.storage.parse_blob_path(blob)
        key = (parts.company, parts.policy, parts.timestamp)
        meta = container.storage.adapter.load_metadata(blob)
        if isinstance(parts, RunBlobPath) and parts.run_id == "latest":
            continue
        blobs.setdefault(key, []).append(parts)
        metas.setdefault(key, []).append(meta)

    missing_brief_count = 0
    missing_brief_files = []
    missing_summary_count = 0
    missing_summary_files = []
    for key, bbs in blobs.items():
        if not any((b.stage == Stage.BRIEF_CLEAN.value for b in bbs)):
            missing_brief_count += 1
            missing_brief_files.append(key)
        if not any((b.stage == Stage.SUMMARY_CLEAN.value for b in bbs)):
            missing_summary_count += 1
            missing_summary_files.append(key)

    loader = SummaryDataLoader(container.storage)
    evals = {version: loader.load_blob_keys(version) for version in loader.find_all_versions()}
    evals_missing = {}
    for version, keys in evals.items():
        for key in keys:
            if key not in blobs:
                evals_missing[key] = {version}
            else:
                if not any((b.stage == Stage.BRIEF_CLEAN.value for b in blobs[key])):
                    evals_missing.setdefault(key, {version}).add("brief")
                if not any((b.stage == Stage.SUMMARY_CLEAN.value for b in blobs[key])):
                    evals_missing.setdefault(key, {version}).add("summary")

    evals_missing_files = {k: list(v) for k,v in evals_missing.items()}  # sets are not json serializable
    return {"Missing Briefs Count": missing_brief_count,
            "Missing Brief Files": missing_brief_files,
            "Missing Summary Count": missing_summary_count,
            "Missing Summary Files": missing_summary_files,
            "Evals Missing": evals_missing_files}


def kill_all(workflow_type: str, reason: str = KILL_CIRCUIT):
    """
    Terminate all running orchestrations.
    
    Args:
        workflow_type: Optional workflow type to filter which orchestrations to terminate
        reason: Reason for termination (default: "Manual termination")
    
    Returns:
        dict with count of terminated orchestrations and their details
    """
    # Get all running/pending orchestrations
    tasks = list_tasks(
        workflow_type=workflow_type,
        runtimes="Running", #["Running", "Pending", "Suspended", "ContinuedAsNew"]
    )
    
    if tasks['count'] == 0:
        return {"count": 0, "terminated": [], "message": "No orchestrations to terminate"}
    
    terminated = []
    for task in tasks['tasks']:
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
                url = f"{_get_app_url()}/runtime/webhooks/durabletask/instances/{instance_id}/terminate"
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


class HttpHandler(BaseHTTPRequestHandler):
    def __init__(self, workflow_type, runtimes, *args, **kwargs):
        self.workflow_type = workflow_type
        self.runtimes = runtimes
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """Handle GET requests."""
        if self.path == "/":
            try:
                print(f"[{datetime.now().isoformat()}] Fetching task data...")
                tasks = list_tasks(self.workflow_type, self.runtimes)
                
                response_data = {
                    "data": tasks,
                    "fetched_at": datetime.now().isoformat(),
                    "error": None
                }
                
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                
                response = json.dumps(response_data, indent=2)
                self.wfile.write(response.encode())
                
            except Exception as e:
                print(f"[{datetime.now().isoformat()}] Error fetching data: {e}")
                
                error_data = {
                    "data": [],
                    "fetched_at": datetime.now().isoformat(),
                    "error": str(e)
                }
                
                self.send_response(500)
                self.send_header("Content-type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                
                response = json.dumps(error_data, indent=2)
                self.wfile.write(response.encode())
        elif self.path == "/files":
            try:
                print(f"[{datetime.now().isoformat()}] Fetching files validation data...")
                files_data = validate_files()
                
                response_data = {
                    "data": files_data,
                    "fetched_at": datetime.now().isoformat(),
                    "error": None
                }
                
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                
                response = json.dumps(response_data, indent=2)
                self.wfile.write(response.encode())
                
            except Exception as e:
                print(f"[{datetime.now().isoformat()}] Error fetching files data: {e}")
                
                error_data = {
                    "data": {},
                    "fetched_at": datetime.now().isoformat(),
                    "error": str(e)
                }
                
                self.send_response(500)
                self.send_header("Content-type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                
                response = json.dumps(error_data, indent=2)
                self.wfile.write(response.encode())
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not Found")
    
    def log_message(self, format, *args):
        """Override to customize logging."""
        print(f"[{datetime.now().isoformat()}] {format % args}")


def server(workflow_type: Optional[str] = None, runtimes: Optional[str|list[str]] = None):
    # Create handler with environment parameters
    def handler(*args, **kwargs):
        return HttpHandler(workflow_type, runtimes, *args, **kwargs)
    
    # Start HTTP server
    port = 8000
    server = HTTPServer(("localhost", port), handler)
    print(f"\nDevelopment server running at http://localhost:{port}")
    print(f"  Routes:")
    print(f"    /       - Task data")
    print(f"    /files  - File validation data")
    print("Data is fetched per request (no polling)")
    print("Press Ctrl+C to stop\n")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        server.shutdown()


def list_tasks(workflow_type: Optional[str] = None, runtimes: Optional[str|list[str]] = None) -> dict:
    params = {}

    if runtimes is None:
        runtimes = ["Running", "Pending", "Suspended", "ContinuedAsNew"]
    params["runtimeStatus"] = runtimes

    params['code'] = str(os.environ.get("AZURE_FUNCTION_MASTER_KEY"))
   
    data = _list_tasks_paged(params)

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
    throttled = Counter([t['custom_status'] is not None and 'Throttled' in t.get('custom_status', '') for t in filtered_data])
    waiting = Counter([t['custom_status'] is not None and 'Waiting' in t.get('custom_status', '') for t in filtered_data])
    workflows = Counter([t['input_data'].get('workflow_type') for t in filtered_data])
    companies = Counter([t['input_data'].get('company') for t in filtered_data])

    formatted = dict(
        count = len(filtered_data),
        tasks = filtered_data,
        summary = dict(names = names,
                       statuses = statuses,
                       throttled = throttled,
                       waiting = waiting,
                       workflows = workflows,
                       companies = companies),
    )

    return formatted


def _list_tasks_paged(params, pages = None, next_token=None):
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
        return _list_tasks_paged(params, pages, next_token=next_page)
    else:
        return pages


def _get_app_url():
    return os.environ.get('WEBSITE_HOSTNAME')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                    prog='health_checks',
                    description='List in flight tasks (run az login before)')
    subparsers = parser.add_subparsers(required=True, dest='action')

    parser_monitor = subparsers.add_parser('monitor', help='list running tasks (live)')
    parser_monitor.add_argument("--workflow_type")
    parser_monitor.add_argument("--env", choices=["DEV","PROD"], default="DEV")
    parser_monitor.add_argument("--runtimes", action='append', default=None, choices=df.OrchestrationRuntimeStatus._member_names_)

    parser_tasks = subparsers.add_parser('tasks', help='list running tasks (static)')
    parser_tasks.add_argument("--workflow_type")
    parser_tasks.add_argument("--output")
    parser_tasks.add_argument("--env", choices=["DEV","PROD"], default="DEV")
    parser_tasks.add_argument("--runtimes", action='append', default=None, choices=df.OrchestrationRuntimeStatus._member_names_)

    parser_kill = subparsers.add_parser('kill', help='terminate all running orchestrations')
    parser_kill.add_argument("--workflow_type", required=True, choices=WORKFLOW_CONFIGS, help='only terminate specific workflow type')
    parser_kill.add_argument("--output")
    parser_kill.add_argument("--env", choices=["DEV","PROD"], default="DEV")
    parser_kill.add_argument("--reason", default=KILL_CIRCUIT, help='termination reason')
    parser_kill.set_defaults(func=kill_all)

    args = parser.parse_args()

    os.environ["TARGET_ENV"] = args.env
    load_env_vars()

    if args.action == "monitor":
        server(args.workflow_type, args.runtimes)
        output = None
    elif args.action == "tasks":
        output = list_tasks(args.workflow_type, args.runtimes)
    elif args.action == "kill":
        output = kill_all(args.workflow_type, args.reason)

    if hasattr(args, 'output') and args.output:
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
    else:
        print(json.dumps(output, indent=2))