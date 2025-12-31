
import pickle
import json
import logging
from typing import Optional, Generator
import os
from dotenv import load_dotenv
import azure.functions as func
from azure import durable_functions as df
from src.blob_utils import (parse_blob_path, 
                            set_connection_key, 
                            get_connection_key,
                            upload_text_blob,
                            upload_json_blob,
                            load_metadata,
                            load_blob,
                            load_json_blob)
from src.log_utils import setup_logger
from src.app_utils import http_wrap, pretty_error, AppError
from src.stages import Stage
from src.orchestrator import OrchData

load_dotenv()

app = func.FunctionApp()

logger = setup_logger(__name__, logging.DEBUG)
logging.getLogger('azure').setLevel(logging.WARNING)

set_connection_key()

# @app.route(route="hello_world", auth_level=func.AuthLevel.FUNCTION)
# @http_wrap
# def hello_world(req: func.HttpRequest):
#     logger.debug("Test logs debug")
#     logger.info("Test logs info")
#     logger.warning("Test logs warning")
#     logger.error("Test logs error")


@app.route(route="seed_urls", auth_level=func.AuthLevel.FUNCTION)
@http_wrap
def seed_urls(req: func.HttpRequest) -> func.HttpResponse:
    """Post seed URLs to blob storage for scraping"""
    from src.seeder import seed_urls as seed_main
    seed_main()


# @app.blob_trigger(arg_name="input_blob", 
#                 path="documents/test_failure_trigger.txt",
#                 connection=get_connection_key())
# @app.durable_client_input(client_name="client")
# @pretty_error
# async def test_failure_trigger(input_blob: func.InputStream, client: df.DurableOrchestrationClient):
#     from datetime import datetime
#     task_id = "persistent_failure_" + datetime.now().isoformat()
#     logger.info(f"Initiating orchestration for {task_id}")
#     orchestration_input = OrchData(task_id, "test_fail", "acme").to_dict()
#     await client.start_new("test_failure_orchestrator", None, orchestration_input)


# @app.activity_trigger(input_name="input_data")
# @pretty_error(retryable=True)
# def test_failure_processor(input_data: dict):
#     task_id = input_data['task_id']
#     logger.info(f"Running processor {task_id}")
#     raise RuntimeError(f"Test processor fail {task_id}")


# @app.orchestration_trigger(context_name="context")
# @pretty_error
# def test_failure_orchestrator(context: df.DurableOrchestrationContext):
#     from src.orchestrator import orchestrator_logic, WorkflowConfig
#     return orchestrator_logic(context, {"test_fail": WorkflowConfig(60,60,5,"test_failure_processor", 1, 5)})


@app.blob_trigger(arg_name="input_blob", 
                path="documents/static_urls.json",
                connection=get_connection_key())
@app.durable_client_input(client_name="client")
@pretty_error
async def meta_blob_trigger(input_blob: func.InputStream, client: df.DurableOrchestrationClient) -> None:
    """Initiate wayback snapshots from static URL list"""
    from src.scraper_utils import load_urls
    blob_name = input_blob.name.removeprefix("documents/")
    urls = load_urls(blob_name)
    for company, url_list in urls.items():
        for url in url_list:
            orchestration_input = OrchData(url, "meta", company).to_dict()
            logger.info(f"Initiating orchestration for {company}/{url}")
            await client.start_new("orchestrator", None, orchestration_input)
    

@app.activity_trigger(input_name="input_data")
@pretty_error(retryable=True)
def meta_processor(input_data: dict) -> None:
    from src.metadata_scraper import scrape_wayback_metadata
    scrape_wayback_metadata(input_data['task_id'], input_data['company'])
    logger.info(f"Successfully scraped: {input_data['task_id']}")


@app.blob_trigger(arg_name="input_blob", 
                path="documents/01-metadata/{company}/{policy}/metadata.json",
                connection=get_connection_key())
@app.durable_client_input(client_name="client")
@pretty_error
async def scraper_blob_trigger(input_blob: func.InputStream, 
                               client: df.DurableOrchestrationClient) -> None:
    """Blob trigger that starts the scraper workflow orchestration."""
    from src.metadata_scraper import parse_wayback_metadata, sample_wayback_metadata
    meta_blob_name = input_blob.name.removeprefix("documents/")
    parts = parse_blob_path(meta_blob_name)
    
    # Parse and re-save metadata
    metadata = parse_wayback_metadata(parts.company, parts.policy)
    
    # Sample metadata for seeding initial db
    metadata = sample_wayback_metadata(metadata, parts.company, parts.policy)
    
    # Start a new orchestration that will download each snapshot
    for row in metadata:
        timestamp = row['timestamp']
        original_url = row['original']
        url_key = f"{timestamp}/{original_url}"
        orchestration_input = OrchData(url_key, 
                                       "scraper", 
                                       parts.company, 
                                       parts.policy, 
                                       timestamp).to_dict()
        logger.info(f"Initiating orchestration for {url_key}")
        await client.start_new("orchestrator", None, orchestration_input)


@app.activity_trigger(input_name="input_data")
@pretty_error(retryable=True)
def scraper_processor(input_data: dict) -> Optional[dict]:
    from src.snapshot_scraper import get_wayback_snapshot
    snap_url = input_data['task_id']
    company = input_data['company']
    policy = input_data['policy']
    timestamp = input_data['timestamp']
    get_wayback_snapshot(company, policy, timestamp, snap_url)
    logger.info(f"Successfully scraped {snap_url}")


@app.timer_trigger(arg_name="input_timer",
                   schedule="0 0 * * 1")
@app.durable_client_input(client_name="client")
@pretty_error
async def scraper_scheduled_trigger(input_timer: func.TimerRequest,
                              client: df.DurableOrchestrationClient) -> None:
    from src.scraper_utils import sanitize_urlpath
    import time
    urls = load_json_blob("static_urls.json")
    for company, url_list in urls.items():
        for url in url_list:
            policy = sanitize_urlpath(url)
            timestamp = time.strftime("%Y%m%d%H%M%S")
            orchestration_input = OrchData(url, 
                                       "webscraper", 
                                       company, 
                                       policy, 
                                       timestamp).to_dict()
            logger.info(f"Initiating orchestration for {url}")
            await client.start_new("orchestrator", None, orchestration_input)


@app.activity_trigger(input_name="input_data")
@pretty_error(retryable=True)
def scraper_scheduled_processor(input_data: dict) -> None:
    from src.snapshot_scraper import get_website
    url = input_data['task_id']
    company = input_data['company']
    policy = input_data['policy']
    timestamp = input_data['timestamp']
    get_website(company, policy, timestamp, url)
    logger.info(f"Successfully scraped {company}/{policy}")


@app.blob_trigger(arg_name="input_blob", 
                path="documents/02-snapshots/{company}/{policy}/{timestamp}.html",
                connection=get_connection_key(),
                data_type="string")
@app.blob_output(arg_name="output_blob",
                path="documents/03-doctrees/{company}/{policy}/{timestamp}.json",
                connection=get_connection_key())
@pretty_error
def parse_snap(input_blob: func.InputStream, output_blob: func.Out[str]):
    """Parse html snapshot into hierarchical doctree format."""
    from src.doctree import parse_html
    tree = parse_html(input_blob.read().decode())
    output_blob.set(tree.__repr__())


@app.blob_trigger(arg_name="input_blob", 
                path="documents/03-doctrees/{company}/{policy}/{timestamp}.json",
                connection=get_connection_key(),
                data_type="string")
@app.blob_output(arg_name="output_blob",
                path="documents/04-doclines/{company}/{policy}/{timestamp}.json",
                connection=get_connection_key())
@pretty_error
def annotate_snap(input_blob: func.InputStream, output_blob: func.Out[str]):
    """Annotate doctree with corpus-level metadata."""
    from src.annotator import main as annotate_main
    path = parse_blob_path(input_blob.name)
    lines = annotate_main(path.company, path.policy, path.timestamp, input_blob.read().decode())
    output_blob.set(lines)


@app.blob_trigger(arg_name="input_blob",
                path="documents/04-doclines/{company}/{policy}/{timestamp}.json",
                connection=get_connection_key())
@pretty_error
def single_diff(input_blob: func.InputStream):
    from src.differ import diff_single
    from src.blob_utils import list_blobs
    blob_name = input_blob.name.removeprefix("documents/")
    path = parse_blob_path(blob_name)
    peers = sorted([x for x in list_blobs() if x.startswith(f"{Stage.DOCCHUNK.value}/{path.company}/{path.policy}")])
    idx = peers.index(blob_name)
    if idx >= 1:
        diff_single(peers[idx-1], blob_name)            
    if idx + 1 < len(peers):
        diff_single(blob_name, peers[idx+1])


@app.blob_trigger(arg_name="input_blob", 
                path="documents/05-diffs-raw/{company}/{policy}/{timestamp}.json",
                connection=get_connection_key(),
                data_type="string")
@app.blob_output(arg_name="output_blob",
                path="documents/05-diffs-clean/{company}/{policy}/{timestamp}.json",
                connection=get_connection_key())
@pretty_error
def clean_diffs(input_blob: func.InputStream, output_blob: func.Out[str]):
    from src.differ import has_diff, clean_diff
    blob = input_blob.read().decode()
    if has_diff(blob):
        diff = clean_diff(blob)
        output_blob.set(diff.model_dump_json())


@app.blob_trigger(arg_name="input_blob", 
                path="documents/05-diffs-clean/{company}/{policy}/{timestamp}.json",
                connection=get_connection_key())
@app.durable_client_input(client_name="client")
@pretty_error
async def summarizer_blob_trigger(input_blob: func.InputStream, client: df.DurableOrchestrationClient) -> None:
    """Blob trigger that starts the summarizer workflow orchestration."""
    blob_name = input_blob.name.removeprefix("documents/")
    parts = parse_blob_path(blob_name)
    orchestration_input = OrchData(blob_name, "summarizer", parts.company, parts.policy, parts.timestamp).to_dict()
    logger.info(f"Initiating orchestration for {blob_name}")
    await client.start_new("orchestrator", None, orchestration_input)
    

@app.activity_trigger(input_name="input_data")
@pretty_error(retryable=True)
def summarizer_processor(input_data: dict):
    from src.summarizer import summarize
    blob_name = input_data['task_id']
    in_path = parse_blob_path(blob_name)
    summary, metadata = summarize(blob_name)
    
    out_path = f"{Stage.SUMMARY_RAW.value}/{in_path.company}/{in_path.policy}/{in_path.timestamp}/{metadata['run_id']}.txt"
    upload_text_blob(summary, out_path, metadata=metadata)
    # XXX: There is a race condition here IF you fan out across versions. Would need new orchestrator for updating latest.
    latest_path = f"{Stage.SUMMARY_RAW.value}/{in_path.company}/{in_path.policy}/{in_path.timestamp}/latest.txt"
    upload_text_blob(summary, latest_path, metadata=metadata)
    logger.info(f"Successfully summarized blob: {blob_name}")


@app.blob_trigger(arg_name="input_blob", 
                path="documents/07-summary-raw/{company}/{policy}/{timestamp}/latest.txt",
                connection=get_connection_key(),
                data_type="string")
@pretty_error
def parse_summary(input_blob: func.InputStream):
    from src.claude_utils import validate_output
    blob_name = input_blob.name.removeprefix("documents/")
    in_path = parse_blob_path(blob_name)
    txt = input_blob.read().decode()
    metadata = load_metadata(blob_name)
    schema = pickle.loads(load_blob(os.path.join(Stage.SCHEMA.value, "summary", metadata['schema_version'] + ".pkl")))
    cleaned_txt = validate_output(txt, schema)
    
    out_path = os.path.join(Stage.SUMMARY_CLEAN.value, in_path.company, in_path.policy, in_path.timestamp, f"{metadata['run_id']}.json")
    upload_json_blob(cleaned_txt, out_path, metadata=metadata)
    # XXX: There is a race condition here IF you fan out across versions. Would need new orchestrator for updating latest.
    out_path = os.path.join(Stage.SUMMARY_CLEAN.value, in_path.company, in_path.policy, in_path.timestamp, "latest.json")
    upload_json_blob(cleaned_txt, out_path, metadata=metadata)
    logger.info(f"Successfully validated blob: {blob_name}")


@app.route(route="prompt_experiment", auth_level=func.AuthLevel.FUNCTION)
@http_wrap
def prompt_experiment(req: func.HttpRequest) -> func.HttpResponse:
    from src.prompt_eng import run_experiment
    run_experiment(req.params.get("labels"))
    return func.HttpResponse("OK")


@app.route(route="evaluate_prompts", auth_level=func.AuthLevel.FUNCTION)
@http_wrap
def evaluate_prompts(req: func.HttpRequest) -> func.HttpResponse:
    from src.prompt_eng import prompt_eval
    return func.HttpResponse(prompt_eval(), mimetype="text/html")

    
@app.orchestration_trigger(context_name="context")
@pretty_error
def orchestrator(context: df.DurableOrchestrationContext) -> Generator:
    from src.orchestrator import orchestrator_logic
    return orchestrator_logic(context)


@app.entity_trigger(context_name="context")
@pretty_error
def rate_limiter(context: df.DurableEntityContext) -> None:
    """Generic Durable Entity that implements token bucket rate limiting for different workflows."""
    from src.rate_limiter import rate_limiter_entity
    return rate_limiter_entity(context)


@app.entity_trigger(context_name="context")
@pretty_error
def circuit_breaker(context: df.DurableEntityContext) -> None:
    """Circuit breaker entity to halt processing on systemic failures."""
    from src.circuit_breaker import circuit_breaker_entity
    return circuit_breaker_entity(context)


@app.route(route="check_circuit_breaker", auth_level=func.AuthLevel.FUNCTION)
@app.durable_client_input(client_name="client")
@pretty_error
async def check_circuit_breaker(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    """Read breaker status."""
    from src.circuit_breaker import check_circuit_breaker as check_cb
    from src.orchestrator import WORKFLOW_CONFIGS
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
    from src.circuit_breaker import reset_circuit_breaker as reset_cb
    return await reset_cb(req, client)
