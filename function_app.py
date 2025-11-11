import logging
from src.log_utils import setup_logger
import azure.functions as func
from azure import durable_functions as df
import requests
import json
from src.stages import Stage
from src.blob_utils import parse_blob_path, load_text_blob, upload_text_blob
from src.app_utils import http_wrap, pretty_error
from src.orchestrator import OrchData

app = func.FunctionApp()

logger = setup_logger(__name__, logging.DEBUG)
logging.getLogger('azure').setLevel(logging.WARNING)


@app.route(route="hello_world", auth_level=func.AuthLevel.FUNCTION)
@http_wrap
def hello_world(req: func.HttpRequest):
    logger.debug("Test logs debug")
    logger.info("Test logs info")
    logger.warning("Test logs warning")
    logger.error("Test logs error")


@app.route(route="seed_urls", auth_level=func.AuthLevel.FUNCTION)
@http_wrap
def seed_urls(req: func.HttpRequest) -> func.HttpResponse:
    """Post seed URLs to blob storage for scraping"""
    from src.seeder import seed_urls as seed_main
    seed_main()


@app.blob_trigger(arg_name="input_blob", 
                path="documents/static_urls.json",
                connection="AZURE_STORAGE_CONNECTION_STRING")
@app.durable_client_input(client_name="client")
@pretty_error
async def meta_blob_trigger(input_blob: func.InputStream, client: df.DurableOrchestrationClient):
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
@pretty_error
def meta_processor(input_data: dict):
    from src.metadata_scraper import scrape_wayback_metadata
    scrape_wayback_metadata(input_data['task_id'], input_data['company'])
    logger.info(f"Successfully scraped: {input_data['task_id']}")


@app.blob_trigger(arg_name="input_blob", 
                path="documents/01-metadata/{company}/{policy}/metadata.json",
                connection="AZURE_STORAGE_CONNECTION_STRING")
@app.durable_client_input(client_name="client")
@pretty_error
async def scraper_blob_trigger(input_blob: func.InputStream, 
                               client: df.DurableOrchestrationClient):
    """Blob trigger that starts the scraper workflow orchestration."""
    from src.metadata_scraper import parse_wayback_metadata
    meta_blob_name = input_blob.name.removeprefix("documents/")
    metadata = parse_wayback_metadata(meta_blob_name)
    if metadata is None:
        return
    
    parts = parse_blob_path(meta_blob_name)
    # Start a new orchestration that will download each snapshot
    for timestamp, original_url in zip(metadata['timestamp'], metadata['original']):
        url_key = f"{timestamp}/{original_url}"
        orchestration_input = OrchData(url_key, 
                                       "scraper", 
                                       parts.company, 
                                       parts.policy, 
                                       timestamp).to_dict()
        logger.info(f"Initiating orchestration for {url_key}")
        await client.start_new("orchestrator", None, orchestration_input)
    

@app.activity_trigger(input_name="input_data")
@pretty_error
def scraper_processor(input_data: dict):
    from src.snapshot_scraper import get_wayback_snapshot
    snap_url = input_data['task_id']
    company = input_data['company']
    policy = input_data['policy']
    timestamp = input_data['timestamp']
    get_wayback_snapshot(company, policy, timestamp, snap_url)
    logger.info(f"Successfully scraped {snap_url}")


@app.blob_trigger(arg_name="input_blob", 
                path="documents/02-snapshots/{company}/{policy}/{timestamp}.html",
                connection="AZURE_STORAGE_CONNECTION_STRING",
                data_type="string")
@app.blob_output(arg_name="output_blob",
                path="documents/03-doctrees/{company}/{policy}/{timestamp}.json",
                connection="AZURE_STORAGE_CONNECTION_STRING")
@pretty_error
def parse_snap(input_blob: func.InputStream, output_blob: func.Out[str]):
    """Parse html snapshot into hierarchical doctree format."""
    from src.doctree import parse_html
    tree = parse_html(input_blob.read().decode())
    output_blob.set(tree)


@app.blob_trigger(arg_name="input_blob", 
                path="documents/03-doctrees/{company}/{policy}/{timestamp}.json",
                connection="AZURE_STORAGE_CONNECTION_STRING",
                data_type="string")
@app.blob_output(arg_name="output_blob",
                path="documents/04-doclines/{company}/{policy}/{timestamp}.json",
                connection="AZURE_STORAGE_CONNECTION_STRING")
@pretty_error
def annotate_snap(input_blob: func.InputStream, output_blob: func.Out[str]) -> None:
    """Annotate doctree with corpus-level metadata."""
    from src.annotator import main as annotate_main
    path = parse_blob_path(input_blob.name)
    lines = annotate_main(path.company, path.policy, path.timestamp, input_blob.read().decode())
    output_blob.set(lines)


@app.route(route="batch_diff", auth_level=func.AuthLevel.FUNCTION)
@http_wrap
def batch_diff(req: func.HttpRequest) -> func.HttpResponse:
    # This has to be http-triggered because we cant guarantee input order.
    from src.differ import diff_batch
    diff_batch()


@app.blob_trigger(arg_name="input_blob", 
                path="documents/05-diffs/{company}/{policy}/{timestamp}.json",
                connection="AZURE_STORAGE_CONNECTION_STRING",
                data_type="string")
@app.blob_output(arg_name="output_blob",
                path="documents/06-prompts/{company}/{policy}/{timestamp}.txt",
                connection="AZURE_STORAGE_CONNECTION_STRING")
@pretty_error
def create_summarizer_prompt(input_blob: func.InputStream, output_blob: func.Out[str]):
    """Language model input."""
    from src.summarizer import create_prompt, is_diff
    blob = input_blob.read().decode()
    if is_diff(blob):
        prompt = create_prompt(blob)
        output_blob.set(prompt)


@app.blob_trigger(arg_name="input_blob", 
                path="documents/06-prompts/{company}/{policy}/{timestamp}.txt",
                connection="AZURE_STORAGE_CONNECTION_STRING")
@app.durable_client_input(client_name="client")
@pretty_error
async def summarizer_blob_trigger(input_blob: func.InputStream, client: df.DurableOrchestrationClient):
    """Blob trigger that starts the summarizer workflow orchestration."""
    blob_name = input_blob.name.removeprefix("documents/")
    parts = parse_blob_path(blob_name)
    orchestration_input = OrchData(blob_name, "summarizer", parts.company, parts.policy, parts.timestamp)
    await client.start_new("orchestrator", None, orchestration_input.to_dict())
    

@app.activity_trigger(input_name="input_data")
@pretty_error
def summarizer_processor(input_data: dict):
    from src.summarizer import summarize
        
    blob_name = input_data['task_id']
    prompt = load_text_blob(blob_name)
    
    logger.debug(f"Summarizing {blob_name}")
    summary_result = summarize(prompt)
    
    in_path = parse_blob_path(blob_name)
    out_path = f"{Stage.SUMMARY_RAW.value}/{in_path.company}/{in_path.policy}/{in_path.timestamp}.txt"
    upload_text_blob(summary_result, out_path)
    
    logger.info(f"Successfully summarized blob: {blob_name}")


@app.blob_trigger(arg_name="input_blob", 
                path="documents/07-summary-raw/{company}/{policy}/{timestamp}.txt",
                connection="AZURE_STORAGE_CONNECTION_STRING",
                data_type="string")
@app.blob_output(arg_name="output_blob",
                path="documents/08-summary-clean/{company}/{policy}/{timestamp}.json",
                connection="AZURE_STORAGE_CONNECTION_STRING")
@pretty_error
def parse_summary(input_blob: func.InputStream, output_blob: func.Out[str]):
    from src.summarizer import parse_response_json
    resp = parse_response_json(input_blob.read().decode())
    output_blob.set(json.dumps(resp, indent=2))


@app.route("validate", auth_level=func.AuthLevel.FUNCTION)
@http_wrap
def validate(req: func.HttpRequest) -> func.HttpResponse:
    from src.health import validate_exists
    return validate_exists()
    

@app.route("in_flight", auth_level=func.AuthLevel.FUNCTION)
@http_wrap
def in_flight(req: func.HttpRequest) -> func.HttpResponse:
    from src.orchestrator import WORKFLOW_CONFIGS
    from src.health import list_in_flight

    if hasattr(req, "params") and req.params is not None and "runtimeStatus" in req.params:
        query = req.params["runtimeStatus"]
        if query in df.OrchestrationRuntimeStatus._member_names_:
            runtime_status = query
        else:
            return func.HttpResponse(f"Invalid parameter runtimeStatus={query}. " \
                                     f"Valid params are {df.OrchestrationRuntimeStatus._member_names_}",
                                      status_code=400, mimetype="plain/text")
    else:
        runtime_status = ["Running", "Pending", "Suspended", "ContinuedAsNew"]

    workflow_type = req.params.get("workflow_type")
    
    if workflow_type is not None and workflow_type not in WORKFLOW_CONFIGS:
        return func.HttpResponse(f"Invalid parameter workflow_type={workflow_type}.", status_code=400)
    
    data = list_in_flight(workflow_type, runtime_status)

    formatted = dict(
        count = len(data),
        tasks = data
    )

    return func.HttpResponse(json.dumps(formatted, indent=2), mimetype="application/json")
    
    
@app.orchestration_trigger(context_name="context")
@pretty_error
def orchestrator(context: df.DurableOrchestrationContext):
    from src.orchestrator import orchestrator_logic
    return orchestrator_logic(context)


@app.entity_trigger(context_name="context")
@pretty_error
def rate_limiter(context: df.DurableEntityContext):
    """Generic Durable Entity that implements token bucket rate limiting for different workflows."""
    from src.rate_limiter import rate_limiter_entity
    return rate_limiter_entity(context)


@app.entity_trigger(context_name="context")
@pretty_error
def circuit_breaker(context: df.DurableEntityContext):
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