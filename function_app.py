import azure.functions as func
from azure import durable_functions as df
from datetime import datetime, timedelta, timezone
import json
import logging
from src.log_utils import setup_logger
from src.blob_utils import parse_blob_path, upload_blob, load_text_blob, load_json_blob
from src.rate_limiter import rate_limiter_entity, orchestrator_logic

logger = setup_logger(__name__, logging.DEBUG)
logging.getLogger('azure').setLevel(logging.WARNING)

app = func.FunctionApp()


# Needed for rate-limited steps only
WORKFLOW_CONFIGS = {
    "summarizer": {
        "rate_limit_rpm": 50,
        "entity_name": "summarizer_rate_limiter",
        "orchestrator_name": "summarizer_orchestrator",
        "activity_name": "summarizer_processor"
    },
    "scraper": {
        "rate_limit_rpm": 30,
        "entity_name": "scraper_rate_limiter",
        "orchestrator_name": "scraper_orchestrator",
        "activity_name": "scraper_processor"
    }
}

@app.route(route="hello_world", auth_level=func.AuthLevel.FUNCTION)
def hello_world(req: func.HttpRequest) -> func.HttpResponse:
    logger.debug("Test logs debug")
    logger.info("Test logs info")
    logger.warning("Test logs warning")
    logger.error("Test logs error")
    return _http_wrap(lambda: "Hello world", "test")

@app.route(route="seed_urls", auth_level=func.AuthLevel.FUNCTION)
def seed_urls(req: func.HttpRequest) -> func.HttpResponse:
    """Post seed URLs to blob storage for scraping"""
    from src.seeder import main as seed_main
    return _http_wrap(seed_main, "seed URLs")


@app.route(route="scrape_meta", auth_level=func.AuthLevel.FUNCTION)
def scrape_snap_meta(req: func.HttpRequest) -> func.HttpResponse:
    """Collect wayback snapshots from static URL list"""
    from src.metadata_scraper import get_wayback_metadatas
    return _http_wrap(get_wayback_metadatas, "wayback snapshots")


# Each workflow gets its own orchestrator with specific input handling
@app.orchestration_trigger(context_name="context")
def scraper_orchestrator(context: df.DurableOrchestrationContext):
    """Orchestrator specifically for scraper workflow."""
    input_data = context.get_input()
    logger.debug("Calling generic orchestrator logic with input data: %s", str(input_data))
    return orchestrate(context, input_data)


@app.blob_trigger(arg_name="input_blob", 
                path="documents/wayback-snapshots/{company}/{policy}/metadata.json",
                connection="AZURE_STORAGE_CONNECTION_STRING")
@app.durable_client_input(client_name="client")
async def scraper_blob_trigger(input_blob: func.InputStream, client: df.DurableOrchestrationClient):
    """Blob trigger that starts the scraper workflow orchestration."""
    blob_name = input_blob.name.removeprefix("documents/")
    orchestration_input = {
        "blob_name": blob_name,
        "workflow_type": "scraper"
    }
    instance_id = await client.start_new("scraper_orchestrator", None, orchestration_input)
    logger.info(f"Started scraper orchestration with ID: '{instance_id}' for blob: {blob_name}")
    

@app.activity_trigger(input_name="input_data")
def scraper_processor(input_data: dict) -> str:
    from src.snapshot_scraper import get_wayback_snapshots
        
    try:
        logger.debug("Calling scraper")
        get_wayback_snapshots(input_data['blob_name'])
        logger.info(f"Successfully scraped: {input_data['blob_name']}")
        return "success"
        
    except Exception as e:
        logger.error(f"Error scraping {input_data['blob_name']}: {str(e)}")
        raise


@app.blob_trigger(arg_name="input_blob", 
                path="documents/wayback-snapshots/{company}/{policy}/{timestamp}.html",
                connection="AZURE_STORAGE_CONNECTION_STRING",
                data_type="string")
@app.blob_output(arg_name="output_blob",
                path="documents/parsed/{company}/{policy}/{timestamp}.json",
                connection="AZURE_STORAGE_CONNECTION_STRING")
def parse_snap(input_blob: func.InputStream, output_blob: func.Out[str]) -> None:
    """Parse html snapshot into hierarchical doctree format."""
    from src.doctree import parse_html
    tree = parse_html(input_blob.read().decode())
    output_blob.set(tree)


@app.blob_trigger(arg_name="input_blob", 
                path="documents/parsed/{company}/{policy}/{timestamp}.json",
                connection="AZURE_STORAGE_CONNECTION_STRING",
                data_type="string")
@app.blob_output(arg_name="output_blob",
                path="documents/annotated/{company}/{policy}/{timestamp}.json",
                connection="AZURE_STORAGE_CONNECTION_STRING")
def annotate_snap(input_blob: func.InputStream, output_blob: func.Out[str]) -> None:
    """Annotate doctree with corpus-level metadata."""
    from src.annotator import main as annotate_main
    path = parse_blob_path(input_blob.name)
    lines = annotate_main(path.company, path.policy, path.timestamp, input_blob.read().decode())
    output_blob.set(lines)


@app.route(route="batch_diff", auth_level=func.AuthLevel.FUNCTION)
def batch_diff(req: func.HttpRequest) -> func.HttpResponse:
    # This has to be http-triggered because we cant guarantee input order.
    from src.differ import diff_batch
    return _http_wrap(diff_batch, "batched diffs")


@app.route(route="single_diff", auth_level=func.AuthLevel.FUNCTION)
def single_diff(req: func.HttpRequest) -> func.HttpResponse:
    # This has to be http-triggered becasue idk how to ensure a full-enough
    # batch of htmls exists before allowing this.
    # TODO: Maybe we have a canary file that the batch deletes. And this file
    # will run as a no-op if it exists.
    from src.differ import diff_single
    return _http_wrap(diff_single, "single diff", req.params['blob_name'])


@app.blob_trigger(arg_name="input_blob", 
                path="documents/diff/{company}/{policy}/{timestamp}.json",
                connection="AZURE_STORAGE_CONNECTION_STRING",
                data_type="string")
@app.blob_output(arg_name="output_blob",
                path="documents/prompts/{company}/{policy}/{timestamp}.txt",
                connection="AZURE_STORAGE_CONNECTION_STRING")
def summary_prompt(input_blob: func.InputStream, output_blob: func.Out[str]) -> None:
    """Use language model to summarize diff."""
    from src.summarizer import create_prompt, is_diff
    blob = input_blob.read().decode()
    if is_diff(blob):
        prompt = create_prompt(blob)
        output_blob.set(prompt)


# Each workflow gets its own orchestrator with specific input handling
@app.orchestration_trigger(context_name="context")
def summarizer_orchestrator(context: df.DurableOrchestrationContext):
    """Orchestrator specifically for summarizer workflow."""
    input_data = context.get_input()
    logger.debug("Calling generic orchestrator logic with input data: %s", str(input_data))
    return orchestrate(context, input_data)


@app.blob_trigger(arg_name="input_blob", 
                path="documents/prompts/{company}/{policy}/{timestamp}.txt",
                connection="AZURE_STORAGE_CONNECTION_STRING")
@app.durable_client_input(client_name="client")
async def summarizer_blob_trigger(input_blob: func.InputStream, client: df.DurableOrchestrationClient):
    """Blob trigger that starts the summarizer workflow orchestration."""
    blob_name = input_blob.name.removeprefix("documents/")
    orchestration_input = {
        "blob_name": blob_name,
        "workflow_type": "summarizer"
    }
    instance_id = await client.start_new("summarizer_orchestrator", None, orchestration_input)
    logger.info(f"Started summarizer orchestration with ID: '{instance_id}' for blob: {blob_name}")
    

@app.activity_trigger(input_name="input_data")
def summarizer_processor(input_data: dict) -> str:
    from src.summarizer import summarize
        
    try:
        logger.debug("Loading prompt for summary from: %s", input_data['blob_name'])
        prompt = load_text_blob ('documents', input_data['blob_name'])
        
        logger.debug("Calling summarizer")
        summary_result = summarize(prompt)
        
        in_path = parse_blob_path(input_data['blob_name'])
        out_path = f"summary_raw/{in_path.company}/{in_path.policy}/{in_path.timestamp}.txt"
        logger.debug("Uploading output blob {out_path}")
        upload_blob(summary_result, 'documents', out_path, "text/plain")
        
        logger.info(f"Successfully summarized blob: {input_data['blob_name']}")
        return summary_result
        
    except Exception as e:
        logger.error(f"Error summarizing blob {input_data['blob_name']}: {str(e)}")
        raise


@app.blob_trigger(arg_name="input_blob", 
                path="documents/summary_raw/{company}/{policy}/{timestamp}.txt",
                connection="AZURE_STORAGE_CONNECTION_STRING",
                data_type="string")
@app.blob_output(arg_name="output_blob",
                path="documents/summary_parsed/{company}/{policy}/{timestamp}.json",
                connection="AZURE_STORAGE_CONNECTION_STRING")
def parse_summary(input_blob: func.InputStream, output_blob: func.Out[str]) -> None:
    from src.summarizer import parse_response_json
    resp = parse_response_json(input_blob.read().decode())
    output_blob.set(json.dumps(resp, indent=2))


# Shared Entity Function for Rate Limiting
@app.entity_trigger(context_name="context")
def generic_rate_limiter_entity(context: df.DurableEntityContext):
    """Generic Durable Entity that implements token bucket rate limiting for different workflows."""
    rate_limiter_entity(context, WORKFLOW_CONFIGS)


def orchestrate(context: df.DurableOrchestrationContext, input_data: dict):
    """Generic Orchestrator that enforces rate limiting using the durable entity."""
    orchestrator_logic(context, WORKFLOW_CONFIGS, input_data)


def _http_wrap(task, taskname, *args, **kwargs) -> func.HttpResponse:
    logger.info(f"Starting {task}")
    try:
        task(*args, **kwargs)  
        return func.HttpResponse(f"Successfully processed {taskname}", status_code=200)
    except Exception as e:
        logger.error(f"Error processing {taskname}: {e}")
        return func.HttpResponse(f"Error processing {taskname}: {str(e)}", status_code=500)