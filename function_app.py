
import json
import logging
import os
from typing import Generator, get_args
from dotenv import load_dotenv
import azure.functions as func
from azure import durable_functions as df
from azure.functions.decorators.core import DataType

from schemas.summary.registry import CLASS_REGISTRY
from src.transforms.seeds import STATIC_URLS
from src.utils.log_utils import setup_logger
from src.utils.app_utils import http_wrap, pretty_error
from src.stages import Stage
from src.orchestration.orchestrator import OrchData
from src.container import ServiceContainer, TEnv

load_dotenv()

app = func.FunctionApp()

logger = setup_logger(__name__, logging.DEBUG)
logging.getLogger('azure').setLevel(logging.WARNING)

ENV = os.environ.get("LIFECYCLE_ENV", "DEV")
assert ENV in get_args(TEnv)

container = ServiceContainer.create(os.environ.get("LIFECYCLE_ENV", "DEV"))

@app.orchestration_trigger(context_name="context")
@pretty_error
def orchestrator(context: df.DurableOrchestrationContext) -> Generator:
    from src.orchestration.orchestrator import orchestrator_logic
    return orchestrator_logic(context)


@app.entity_trigger(context_name="context")
@pretty_error
def rate_limiter(context: df.DurableEntityContext) -> None:
    """Generic Durable Entity that implements token bucket rate limiting for different workflows."""
    from src.orchestration.rate_limiter import rate_limiter_entity
    return rate_limiter_entity(context)


@app.entity_trigger(context_name="context")
@pretty_error
def circuit_breaker(context: df.DurableEntityContext) -> None:
    """Circuit breaker entity to halt processing on systemic failures."""
    from src.orchestration.circuit_breaker import circuit_breaker_entity
    return circuit_breaker_entity(context)


@app.route(route="check_circuit_breaker", auth_level=func.AuthLevel.FUNCTION)
@app.durable_client_input(client_name="client")
@pretty_error
async def check_circuit_breaker(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    """Read breaker status."""
    from src.orchestration.circuit_breaker import check_circuit_breaker as check_cb
    from src.orchestration.orchestrator import WORKFLOW_CONFIGS
    if hasattr(req, "params") and req.params is not None and "workflow_type" in req.params:
        workflow_type = req.params["workflow_type"]
        data = await check_cb(workflow_type, client)
    else:
        data = [await check_cb(w, client) for w in WORKFLOW_CONFIGS.keys()]

    return func.HttpResponse(
            json.dumps(data, indent=2),
            mimetype="application/json",
            status_code=200
        )

@app.route(route="reset_circuit_breaker", auth_level=func.AuthLevel.FUNCTION)
@app.durable_client_input(client_name="client")
@pretty_error
async def reset_circuit_breaker(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    """Manually reset a breaker."""
    from src.orchestration.circuit_breaker import reset_circuit_breaker as reset_cb
    return await reset_cb(req, client)


@app.route(route="meta_trigger", auth_level=func.AuthLevel.FUNCTION)
@app.durable_client_input(client_name="client")
@pretty_error
async def meta_trigger(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    """Initiate wayback snapshots from static URL list"""
    urls = STATIC_URLS
    for company, url_list in urls.items():
        for url in url_list:
            orchestration_input = OrchData(url, "meta", company).to_dict()
            logger.info(f"Initiating orchestration for {company}/{url}")
            await client.start_new("orchestrator", None, orchestration_input)
    return func.HttpResponse("OK")


# @app.blob_trigger(arg_name="input_blob",
#                 path="documents/static_urls.json",
#                 connection=container.storage.adapter.get_connection_key())
# @app.durable_client_input(client_name="client")
# @pretty_error
# async def meta_blob_trigger(input_blob: func.InputStream, client: df.DurableOrchestrationClient) -> None:
#     """Initiate wayback snapshots from static URL list"""
#     urls = container.storage.load_json_blob(input_blob.name)
#     for company, url_list in urls.items():
#         for url in url_list:
#             orchestration_input = OrchData(url, "meta", company).to_dict()
#             logger.info(f"Initiating orchestration for {company}/{url}")
#             await client.start_new("orchestrator", None, orchestration_input)
#
#
# @app.activity_trigger(input_name="input_data")
# @pretty_error(retryable=True)
# def meta_processor(input_data: dict) -> None:
#     container.wayback_service.scrape_wayback_metadata(input_data['task_id'], input_data['company'])
#
#
# @app.blob_trigger(arg_name="input_blob",
#                 path="documents/01-metadata/{company}/{policy}/metadata.json",
#                 connection=container.storage.adapter.get_connection_key())
# @app.durable_client_input(client_name="client")
# @pretty_error
# async def scraper_blob_trigger(input_blob: func.InputStream,
#                                client: df.DurableOrchestrationClient) -> None:
#     """Blob trigger that starts the scraper workflow orchestration."""
#     parts = container.storage.parse_blob_path(input_blob.name)
#
#     # Parse and re-save metadata
#     metadata = container.wayback_service.parse_wayback_metadata(parts.company, parts.policy)
#
#     # Sample metadata for seeding initial db
#     metadata = container.wayback_service.sample_wayback_metadata(metadata, parts.company, parts.policy)
#
#     # Start a new orchestration that will download each snapshot
#     for row in metadata:
#         timestamp = row['timestamp']
#         original_url = row['original']
#         url_key = f"{timestamp}/{original_url}"
#         orchestration_input = OrchData(url_key,
#                                        "scraper",
#                                        parts.company,
#                                        parts.policy,
#                                        timestamp).to_dict()
#         logger.info(f"Initiating orchestration for {url_key}")
#         await client.start_new("orchestrator", None, orchestration_input)
#
#
# @app.activity_trigger(input_name="input_data")
# @pretty_error(retryable=True)
# def scraper_processor(input_data: dict) -> None:
#     snap_url = input_data['task_id']
#     company = input_data['company']
#     policy = input_data['policy']
#     timestamp = input_data['timestamp']
#     container.snapshot_service.get_wayback_snapshot(company, policy, timestamp, snap_url)
#     logger.info(f"Successfully scraped {snap_url}")
#
#
# @app.timer_trigger(arg_name="input_timer",
#                    schedule="0 0 * * 1")
# @app.durable_client_input(client_name="client")
# @pretty_error
# async def scraper_scheduled_trigger(input_timer: func.TimerRequest,
#                               client: df.DurableOrchestrationClient) -> None:
#     from src.utils.path_utils import extract_policy
#     import time
#     urls = container.storage.load_json_blob("static_urls.json")
#     for company, url_list in urls.items():
#         for url in url_list:
#             policy = extract_policy(url)
#             timestamp = time.strftime("%Y%m%d%H%M%S")
#             orchestration_input = OrchData(url,
#                                        "webscraper",
#                                        company,
#                                        policy,
#                                        timestamp).to_dict()
#             logger.info(f"Initiating orchestration for {url}")
#             await client.start_new("orchestrator", None, orchestration_input)
#
#
# @app.activity_trigger(input_name="input_data")
# @pretty_error(retryable=True)
# def scraper_scheduled_processor(input_data: dict) -> None:
#     url = input_data['task_id']
#     company = input_data['company']
#     policy = input_data['policy']
#     timestamp = input_data['timestamp']
#     container.snapshot_service.get_website(company, policy, timestamp, url)
#     logger.info(f"Successfully scraped {company}/{policy}")
#
#
# @app.blob_trigger(arg_name="input_blob",
#                 path="documents/02-snapshots/{company}/{policy}/{timestamp}.html",
#                 connection=container.storage.adapter.get_connection_key(),
#                 data_type=DataType.STRING)
# @app.blob_output(arg_name="output_blob",
#                 path="documents/03-doctrees/{company}/{policy}/{timestamp}.json",
#                 connection=container.storage.adapter.get_connection_key())
# @pretty_error
# def parse_snap(input_blob: func.InputStream, output_blob: func.Out[str]):
#     """Parse html snapshot into hierarchical doctree format."""
#     from src.transforms.doctree import parse_html
#     tree = parse_html(input_blob.read().decode())
#     output_blob.set(tree.__repr__())
#
#
# @app.blob_trigger(arg_name="input_blob",
#                 path="documents/03-doctrees/{company}/{policy}/{timestamp}.json",
#                 connection=container.storage.adapter.get_connection_key(),
#                 data_type=DataType.STRING)
# @app.blob_output(arg_name="output_blob",
#                 path="documents/04-doclines/{company}/{policy}/{timestamp}.json",
#                 connection=container.storage.adapter.get_connection_key())
# @pretty_error
# def annotate_snap(input_blob: func.InputStream, output_blob: func.Out[str]):
#     """Annotate doctree with corpus-level metadata."""
#     from src.transforms.annotator import annotate_and_pool
#     path = container.storage.parse_blob_path(input_blob.name)
#     lines = annotate_and_pool(path.company, path.policy, path.timestamp, input_blob.read().decode())
#     output_blob.set(lines)
#
#
# @app.blob_trigger(arg_name="input_blob",
#                 path="documents/04-doclines/{company}/{policy}/{timestamp}.json",
#                 connection=container.storage.adapter.get_connection_key())
# @pretty_error
# def single_diff(input_blob: func.InputStream):
#     differ = container.differ_service
#     blob_name = input_blob.name.removeprefix("documents/")
#     differ.diff_and_save(blob_name)
#
# @app.blob_trigger(arg_name="input_blob",
#                 path="documents/05-diffs-raw/{company}/{policy}/{timestamp}.json",
#                 connection=container.storage.adapter.get_connection_key(),
#                 data_type=DataType.STRING)
# @app.blob_output(arg_name="output_blob",
#                 path="documents/05-diffs-clean/{company}/{policy}/{timestamp}.json",
#                 connection=container.storage.adapter.get_connection_key())
# @pretty_error
# def clean_diffs(input_blob: func.InputStream, output_blob: func.Out[str]):
#     blob = input_blob.read().decode()
#     if container.differ_service.has_diff(blob):
#         diff = container.differ_service.clean_diff(blob)
#         output_blob.set(diff.model_dump_json())
#
#
# @app.blob_trigger(arg_name="input_blob",
#                 path="documents/05-diffs-clean/{company}/{policy}/{timestamp}.json",
#                 connection=container.storage.adapter.get_connection_key())
# @app.durable_client_input(client_name="client")
# @pretty_error
# async def summarizer_blob_trigger(input_blob: func.InputStream, client: df.DurableOrchestrationClient) -> None:
#     """Blob trigger that starts the summarizer workflow orchestration."""
#     blob_name = input_blob.name.removeprefix("documents/")
#     parts = container.storage.parse_blob_path(blob_name)
#     orchestration_input = OrchData(blob_name, "summarizer", parts.company, parts.policy, parts.timestamp).to_dict()
#     logger.info(f"Initiating orchestration for {blob_name}")
#     await client.start_new("orchestrator", None, orchestration_input)
#
#
# @app.activity_trigger(input_name="input_data")
# @pretty_error(retryable=True)
# def summarizer_processor(input_data: dict):
#     blob_name = input_data['task_id']
#     in_path = container.storage.parse_blob_path(blob_name)
#     summary, metadata = container.summarizer_service.summarize(blob_name)
#
#     # XXX: There is a race condition here IF you fan out across experiments. Would need new orchestrator for updating latest.
#     out_path = f"{Stage.SUMMARY_RAW.value}/{in_path.company}/{in_path.policy}/{in_path.timestamp}/{metadata['run_id']}.txt"
#     container.storage.upload_text_blob(summary, out_path, metadata=metadata)
#     latest_path = f"{Stage.SUMMARY_RAW.value}/{in_path.company}/{in_path.policy}/{in_path.timestamp}/latest.txt"
#     container.storage.upload_text_blob(summary, latest_path, metadata=metadata)
#     logger.info(f"Successfully summarized blob: {blob_name}")
#
#
# @app.blob_trigger(arg_name="input_blob",
#                 path="documents/07-summary-raw/{company}/{policy}/{timestamp}/latest.txt",
#                 connection=container.storage.adapter.get_connection_key(),
#                 data_type=DataType.STRING)
# @pretty_error
# def parse_summary(input_blob: func.InputStream):
#     blob_name = input_blob.name.removeprefix("documents/")
#     in_path = container.storage.parse_blob_path(blob_name)
#     txt = input_blob.read().decode()
#     metadata = container.storage.adapter.load_metadata(blob_name)
#     schema = CLASS_REGISTRY[metadata['schema_version']]
#     cleaned_txt = container.summarizer_service.llm.validate_output(txt, schema)
#
#     out_path = os.path.join(Stage.SUMMARY_CLEAN.value, in_path.company, in_path.policy, in_path.timestamp, f"{metadata['run_id']}.json")
#     container.storage.upload_json_blob(cleaned_txt, out_path, metadata=metadata)
#     # XXX: There is a race condition here IF you fan out across versions. Would need new orchestrator for updating latest.
#     out_path = os.path.join(Stage.SUMMARY_CLEAN.value, in_path.company, in_path.policy, in_path.timestamp, "latest.json")
#     container.storage.upload_json_blob(cleaned_txt, out_path, metadata=metadata)
#     logger.info(f"Successfully validated blob: {blob_name}")
#
#
# @app.route(route="prompt_experiment", auth_level=func.AuthLevel.FUNCTION)
# @http_wrap
# def prompt_experiment(req: func.HttpRequest) -> func.HttpResponse:
#     from src.prompt_eng import run_experiment
#     run_experiment(req.params.get("labels"))
#     return func.HttpResponse("OK")
#
#
# @app.route(route="evaluate_prompts", auth_level=func.AuthLevel.FUNCTION)
# @http_wrap
# def evaluate_prompts(req: func.HttpRequest) -> func.HttpResponse:
#     from src.prompt_eng import prompt_eval
#     return func.HttpResponse(prompt_eval(), mimetype="text/html")
#
#
