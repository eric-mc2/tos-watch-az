import azure.functions as func
from azure import durable_functions as df
from datetime import datetime, timedelta, timezone
import json
import logging
from src.log_utils import setup_logger
from src.blob_utils import parse_blob_path, upload_blob, load_text_blob

logger = setup_logger(logging.DEBUG)

app = func.FunctionApp()


# Needed for rate-limited steps only
WORKFLOW_CONFIGS = {
    "summarizer": {
        "rate_limit_rpm": 50,
        "entity_name": "rate_limiter_entity_summarizer",
        "orchestrator_name": "rate_limited_orchestrator_summarizer",
        "activity_name": "summarizer_processor"
    },
    "scraper": {
        "rate_limit_rpm": 30,  # Different rate limit
        "entity_name": "rate_limiter_entity_scraper",
        "orchestrator_name": "rate_limited_orchestrator_scraper",
        "activity_name": "scraper"
    }
}


@app.route(route="seed_urls", auth_level=func.AuthLevel.FUNCTION)
def seed_urls(req: func.HttpRequest) -> func.HttpResponse:
    """Post seed URLs to blob storage for scraping"""
    from src.seeder import main as seed_main
    return _http_wrap(seed_main, "seed URLs")


@app.route(route="scrape", auth_level=func.AuthLevel.FUNCTION)
def scrape_snaps(req: func.HttpRequest) -> func.HttpResponse:
    """Collect wayback snapshots from static URL list"""
    from src.scraper import get_wayback_snapshots
    return _http_wrap(get_wayback_snapshots, "wayback snapshots")


# TODO: Implement these using sumamrizer as model
# @app.durable_client_input(client_name="client")
# def scraper_trigger_handler(req: func.HttpRequest, client: df.DurableOrchestrationClient):
#     """Trigger that starts the scraper input workflow orchestration."""
#     TODO: Wrap in for loop over urls?
#     orchestration_input = {
#         "blob_name": input_blob.name,
#         "blob_uri": input_blob.uri,
#         "workflow_type": "scraper"
#     }
    
#     config = WORKFLOW_CONFIGS["scraper"]
#     instance_id = client.start_new(config["orchestrator_name"], None, orchestration_input)
#     logger.info(f"Started scraper input orchestration with ID: '{instance_id}' for blob: {input_blob.name}")


# @app.activity_trigger(input_name="activity_input")
# @app.blob_output(arg_name="output_blob",
#                 path=WORKFLOW_CONFIGS["scraper"]["output_path"],
#                 connection="AZURE_STORAGE_CONNECTION_STRING")
# def process_blob_activity_scraper(activity_input: str, output_blob: func.Out[str]) -> str:
#     """Activity function that processes blobs to scrape."""
#     from src.scraper import main as scraper_main  # Different processing function
    
#     # Parse input data
#     if isinstance(activity_input, str):
#         input_data = json.loads(activity_input)
#     else:
#         input_data = activity_input
    
#     try:
#         # Call different external service for processing
#         content = load_json_blob('documents', input_data['blob_name'])
#         processing_result = scraper_main(content)  # Different processing logic
        
#         # Write result to output blob
#         output_blob.set(processing_result)
        
#         logger.info(f"Successfully scraped input blob: {input_data['blob_name']}")
#         return processing_result
        
#     except Exception as e:
#         logger.error(f"Error scraping input blob {input_data['blob_name']}: {str(e)}")
#         raise



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


# Shared Entity Function for Rate Limiting
@app.entity_trigger(context_name="context")
def generic_rate_limiter_entity(context: df.DurableEntityContext):
    """Generic Durable Entity that implements token bucket rate limiting for different workflows."""
    entity_name = context.entity_key
    
    # Get configuration for this entity
    config = None
    for workflow, workflow_config in WORKFLOW_CONFIGS.items():
        if workflow_config["entity_name"] == entity_name:
            config = workflow_config
            break

    if not config:
        logger.warning(f"Unknown rate limiter entity key {entity_name}")
        context.set_result(False)
        return
    
    rate_limit_rpm = config["rate_limit_rpm"]
    
    current_state = context.get_state(lambda: {
        "tokens": rate_limit_rpm,
        "last_refill": None,
    })
    
    current_time_str = context.get_input()
    current_time = datetime.fromisoformat(current_time_str) if current_time_str else datetime.now(timezone.utc)
    
    if current_state["last_refill"] is None:
        current_state["last_refill"] = current_time.isoformat()
        current_state["tokens"] = rate_limit_rpm
    
    else:
        last_refill = datetime.fromisoformat(current_state["last_refill"])
        
        # Calculate time elapsed since last refill
        time_elapsed = (current_time - last_refill).total_seconds()
        
        # Refill tokens based on elapsed time
        if time_elapsed >= 60:
            current_state["tokens"] = rate_limit_rpm
            current_state["last_refill"] = current_time.isoformat()
        
    operation = context.operation_name
    
    if operation == "try_consume":
        if current_state["tokens"] > 0:
            current_state["tokens"] -= 1
            context.set_result(True)
        else:
            context.set_result(False)
    
    elif operation == "get_status":
        context.set_result(current_state)
    
    context.set_state(current_state)
    logger.debug(f"Rate limiter finished with result: {context._result} and state: {context.get_state()}")


# Shared Orchestrator Function
def generic_rate_limited_orchestrator_logic(context: df.DurableOrchestrationContext, input_data: dict):
    """Generic Orchestrator that enforces rate limiting using the durable entity."""
    input_data = context.get_input()
    workflow_type = input_data.get("workflow_type")
    
    if workflow_type not in WORKFLOW_CONFIGS:
        raise ValueError(f"Unknown workflow type: {workflow_type}")
    
    config = WORKFLOW_CONFIGS[workflow_type]
    entity_id = df.EntityId("generic_rate_limiter_entity", config["entity_name"])
    
    logger.debug(f"Executing orchestrator logic -> entity: {config['entity_name']}")

    # Wait for rate limit token
    while True:
        allowed = yield context.call_entity(entity_id, "try_consume", context.current_utc_datetime.isoformat())
        if allowed:
            break
        # Wait before retrying
        retry_time = context.current_utc_datetime + timedelta(seconds=5)
        yield context.create_timer(retry_time)
    
    logger.debug("Orchestrator passed rate limiter and calling next activity: %s", config["activity_name"])
    # Process the blob with acquired rate token
    result = yield context.call_activity(config["activity_name"], input_data)
    return result


# Each workflow gets its own orchestrator with specific input handling
@app.orchestration_trigger(context_name="context")
def rate_limited_orchestrator_summarizer(context: df.DurableOrchestrationContext):
    """Orchestrator specifically for summarizer workflow."""
    input_data = context.get_input()
    logger.debug("Calling generic orchestrator logic with input data: %s", str(input_data))
    return generic_rate_limited_orchestrator_logic(context, input_data)


@app.orchestration_trigger(context_name="context")  
def rate_limited_orchestrator_scraper(context: df.DurableOrchestrationContext):
    """Orchestrator for scraper workflow."""
    input_data = context.get_input()
    input_data["workflow_type"] = "scraper"
    return generic_rate_limited_orchestrator_logic(context, input_data)


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
    instance_id = await client.start_new("rate_limited_orchestrator_summarizer", None, orchestration_input)
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

def _http_wrap(task, taskname, *args, **kwargs) -> func.HttpResponse:
    logger.info(f"Starting {task}")
    try:
        task(*args, **kwargs)  
        return func.HttpResponse(f"Successfully processed {taskname}", status_code=200)
    except Exception as e:
        logger.error(f"Error processing {taskname}: {e}")
        return func.HttpResponse(f"Error processing {taskname}: {str(e)}", status_code=500)