import azure.functions as func
from azure import durable_functions as df
import json
import logging
from src.stages import Stage
from src.log_utils import setup_logger
from src.blob_utils import parse_blob_path, upload_blob, load_text_blob, load_json_blob, upload_text_blob
from src.rate_limiter import rate_limiter_entity, orchestrator_logic, circuit_breaker_entity

logger = setup_logger(__name__, logging.DEBUG)
logging.getLogger('azure').setLevel(logging.WARNING)

app = func.FunctionApp()


# Needed for rate-limited steps only
WORKFLOW_CONFIGS = {
    "summarizer": {
        "rate_limit_rpm": 50,
        "activity_name": "summarizer_processor",
        "max_retries": 3
    },
    "scraper": {
        "rate_limit_rpm": 5,  # Reduced from 10 to be more conservative
        "activity_name": "scraper_processor",
        "max_retries": 3
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
    from src.seeder import seed_urls as seed_main
    return _http_wrap(seed_main, "seed URLs")


@app.route(route="scrape_meta", auth_level=func.AuthLevel.FUNCTION)
def scrape_snap_meta(req: func.HttpRequest) -> func.HttpResponse:
    """Collect wayback snapshots from static URL list"""
    from src.metadata_scraper import get_wayback_metadatas
    return _http_wrap(get_wayback_metadatas, "wayback snapshots")


@app.blob_trigger(arg_name="input_blob", 
                path=f"documents/{Stage.SNAP.value}/{{company}}/{{policy}}/metadata.json",
                connection="AZURE_STORAGE_CONNECTION_STRING")
@app.durable_client_input(client_name="client")
async def scraper_blob_trigger(input_blob: func.InputStream, client: df.DurableOrchestrationClient):
    """Blob trigger that starts the scraper workflow orchestration."""
    blob_name = input_blob.name.removeprefix("documents/")
    orchestration_input = {
        "blob_name": blob_name, 
        "workflow_type": "scraper"
    }
    await client.start_new("generic_orchestrator", None, orchestration_input)
    

@app.activity_trigger(input_name="input_data")
def scraper_processor(input_data: dict) -> str:
    from src.snapshot_scraper import get_wayback_snapshots
    try:
        get_wayback_snapshots(input_data['blob_name'])
        logger.info(f"Successfully scraped: {input_data['blob_name']}")
        return "success"
    except Exception as e:
        logger.error(f"Error scraping {input_data['blob_name']}: {e}")
        raise


@app.blob_trigger(arg_name="input_blob", 
                path=f"documents/{Stage.SNAP.value}/{{company}}/{{policy}}/{{timestamp}}.html",
                connection="AZURE_STORAGE_CONNECTION_STRING",
                data_type="string")
@app.blob_output(arg_name="output_blob",
                path=f"documents/{Stage.DOCTREE.value}/{{company}}/{{policy}}/{{timestamp}}.json",
                connection="AZURE_STORAGE_CONNECTION_STRING")
def parse_snap(input_blob: func.InputStream, output_blob: func.Out[str]) -> None:
    """Parse html snapshot into hierarchical doctree format."""
    from src.doctree import parse_html
    tree = parse_html(input_blob.read().decode())
    output_blob.set(tree)


@app.blob_trigger(arg_name="input_blob", 
                path=f"documents/{Stage.DOCTREE.value}/{{company}}/{{policy}}/{{timestamp}}.json",
                connection="AZURE_STORAGE_CONNECTION_STRING",
                data_type="string")
@app.blob_output(arg_name="output_blob",
                path=f"documents/{Stage.DOCCHUNK.value}/{{company}}/{{policy}}/{{timestamp}}.json",
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
                path=f"documents/{Stage.DIFF.value}/{{company}}/{{policy}}/{{timestamp}}.json",
                connection="AZURE_STORAGE_CONNECTION_STRING",
                data_type="string")
@app.blob_output(arg_name="output_blob",
                path=f"documents/{Stage.PROMPT.value}/{{company}}/{{policy}}/{{timestamp}}.txt",
                connection="AZURE_STORAGE_CONNECTION_STRING")
def create_summarizer_prompt(input_blob: func.InputStream, output_blob: func.Out[str]) -> None:
    """Use language model to summarize diff."""
    from src.summarizer import create_prompt, is_diff
    blob = input_blob.read().decode()
    if is_diff(blob):
        prompt = create_prompt(blob)
        output_blob.set(prompt)


@app.orchestration_trigger(context_name="context")
def generic_orchestrator(context: df.DurableOrchestrationContext):
    return orchestrator_logic(context, WORKFLOW_CONFIGS)


@app.blob_trigger(arg_name="input_blob", 
                path=f"documents/{Stage.PROMPT.value}/{{company}}/{{policy}}/{{timestamp}}.txt",
                connection="AZURE_STORAGE_CONNECTION_STRING")
@app.durable_client_input(client_name="client")
async def summarizer_blob_trigger(input_blob: func.InputStream, client: df.DurableOrchestrationClient):
    """Blob trigger that starts the summarizer workflow orchestration."""
    blob_name = input_blob.name.removeprefix("documents/")
    orchestration_input = {
        "blob_name": blob_name,
        "workflow_type": "summarizer"
    }
    await client.start_new("generic_orchestrator", None, orchestration_input)
    

@app.activity_trigger(input_name="input_data")
def summarizer_processor(input_data: dict) -> str:
    from src.summarizer import summarize
        
    try:
        blob_name = input_data['blob_name']
        prompt = load_text_blob('documents', blob_name)
        
        logger.debug(f"Summarizing {blob_name}")
        summary_result = summarize(prompt)
        
        in_path = parse_blob_path(blob_name)
        out_path = f"{Stage.SUMMARY_RAW.value}/{in_path.company}/{in_path.policy}/{in_path.timestamp}.txt"
        upload_text_blob(summary_result, 'documents', out_path)
        
        logger.info(f"Successfully summarized blob: {blob_name}")
        return summary_result
        
    except Exception as e:
        blob_name = input_data.get('blob_name', 'unknown')
        logger.error(f"Error summarizing blob {blob_name}: {e}")
        raise


@app.blob_trigger(arg_name="input_blob", 
                path=f"documents/{Stage.SUMMARY_RAW.value}/{{company}}/{{policy}}/{{timestamp}}.txt",
                connection="AZURE_STORAGE_CONNECTION_STRING",
                data_type="string")
@app.blob_output(arg_name="output_blob",
                path=f"documents/{Stage.SUMMARY_CLEAN.value}/{{company}}/{{policy}}/{{timestamp}}.json",
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


@app.entity_trigger(context_name="context")
def circuit_breaker_entity_func(context: df.DurableEntityContext):
    """Circuit breaker entity to halt processing on systemic failures."""
    circuit_breaker_entity(context)


@app.route(route="reset_circuit_breaker", auth_level=func.AuthLevel.FUNCTION)
@app.durable_client_input(client_name="client")
async def reset_circuit_breaker(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    """Manually reset a circuit breaker for a workflow type."""
    workflow_type = req.params.get('workflow_type', 'scraper')
    
    entity_id = df.EntityId("circuit_breaker_entity_func", workflow_type)
    
    # Check if entity exists first
    entity_state = await client.read_entity_state(entity_id)
    
    if not entity_state.entity_exists:
        # Entity doesn't exist yet, just return success since there's nothing to reset
        logger.info(f"Circuit breaker for {workflow_type} doesn't exist yet (never initialized)")
        return func.HttpResponse(
            f"Circuit breaker for {workflow_type} doesn't exist yet (no orchestrations have run)", 
            status_code=200
        )
    
    # Entity exists, signal it to reset
    await client.signal_entity(entity_id, "reset")
    
    logger.info(f"Circuit breaker reset for workflow: {workflow_type}")
    return func.HttpResponse(f"Circuit breaker reset for {workflow_type}", status_code=200)


def _http_wrap(task, taskname, *args, **kwargs) -> func.HttpResponse:
    logger.info(f"Starting {task}")
    try:
        task(*args, **kwargs)  
        return func.HttpResponse(f"Successfully processed {taskname}", status_code=200)
    except Exception as e:
        logger.error(f"Error processing {taskname}: {e}")
        return func.HttpResponse(f"Error processing {taskname}: {e}", status_code=500)