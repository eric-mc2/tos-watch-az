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
                path=f"documents/static_urls.json",
                connection="AZURE_STORAGE_CONNECTION_STRING")
@app.durable_client_input(client_name="client")
@pretty_error
async def meta_blob_trigger(input_blob: func.InputStream, client: df.DurableOrchestrationClient):
    """Initiate wayback snapshots from static URL list"""
    from src.scraper_utils import load_urls
    urls = load_urls(input_blob.name)
    for company, url_list in urls.items():
        for url in url_list:
            orchestration_input = OrchData(company, url, "meta").to_dict()
            logger.info(f"Initiating orchestration for {company}/{url}")
            await client.start_new("orchestrator", None, orchestration_input)
    

@app.activity_trigger(input_name="input_data")
@pretty_error
def meta_processor(input_data: dict):
    from src.metadata_scraper import scrape_wayback_metadata
    scrape_wayback_metadata(input_data['task_id'], input_data['company'])
    logger.info(f"Successfully scraped: {input_data['task_id']}")

# @http_wrap
# @app.blob_trigger(arg_name="input_blob", 
#                 path=f"documents/{Stage.SNAP.value}/{{company}}/{{policy}}/metadata.json",
#                 connection="AZURE_STORAGE_CONNECTION_STRING")
# @app.durable_client_input(client_name="client")
# async def scraper_blob_trigger(input_blob: func.InputStream, client: df.DurableOrchestrationClient):
#     """Blob trigger that starts the scraper workflow orchestration."""
#     blob_name = input_blob.name.removeprefix("documents/")
#     orchestration_input = {
#         "task_id": blob_name, 
#         "workflow_type": "scraper"
#     }
#     await client.start_new("orchestrator", None, orchestration_input)
    

# @http_wrap
# @app.activity_trigger(input_name="input_data")
# def scraper_processor(input_data: dict) -> str:
#     from src.snapshot_scraper import get_wayback_snapshots
#     try:
#         get_wayback_snapshots(input_data['task_id'])
#         logger.info(f"Successfully scraped: {input_data['task_id']}")
#         return "success"
#     except Exception as e:
#         logger.error(f"Error scraping {input_data['task_id']}: {e}")
#         raise


# @http_wrap
# @app.blob_trigger(arg_name="input_blob", 
#                 path=f"documents/{Stage.SNAP.value}/{{company}}/{{policy}}/{{timestamp}}.html",
#                 connection="AZURE_STORAGE_CONNECTION_STRING",
#                 data_type="string")
# @app.blob_output(arg_name="output_blob",
#                 path=f"documents/{Stage.DOCTREE.value}/{{company}}/{{policy}}/{{timestamp}}.json",
#                 connection="AZURE_STORAGE_CONNECTION_STRING")
# def parse_snap(input_blob: func.InputStream, output_blob: func.Out[str]) -> None:
#     """Parse html snapshot into hierarchical doctree format."""
#     from src.doctree import parse_html
#     tree = parse_html(input_blob.read().decode())
#     output_blob.set(tree)


# @http_wrap
# @app.blob_trigger(arg_name="input_blob", 
#                 path=f"documents/{Stage.DOCTREE.value}/{{company}}/{{policy}}/{{timestamp}}.json",
#                 connection="AZURE_STORAGE_CONNECTION_STRING",
#                 data_type="string")
# @app.blob_output(arg_name="output_blob",
#                 path=f"documents/{Stage.DOCCHUNK.value}/{{company}}/{{policy}}/{{timestamp}}.json",
#                 connection="AZURE_STORAGE_CONNECTION_STRING")
# def annotate_snap(input_blob: func.InputStream, output_blob: func.Out[str]) -> None:
#     """Annotate doctree with corpus-level metadata."""
#     from src.annotator import main as annotate_main
#     path = parse_blob_path(input_blob.name)
#     lines = annotate_main(path.company, path.policy, path.timestamp, input_blob.read().decode())
#     output_blob.set(lines)


# @http_wrap
# @app.route(route="batch_diff", auth_level=func.AuthLevel.FUNCTION)
# def batch_diff(req: func.HttpRequest) -> func.HttpResponse:
#     # This has to be http-triggered because we cant guarantee input order.
#     from src.differ import diff_batch
#     diff_batch()



# @http_wrap
# @app.blob_trigger(arg_name="input_blob", 
#                 path=f"documents/{Stage.DIFF.value}/{{company}}/{{policy}}/{{timestamp}}.json",
#                 connection="AZURE_STORAGE_CONNECTION_STRING",
#                 data_type="string")
# @app.blob_output(arg_name="output_blob",
#                 path=f"documents/{Stage.PROMPT.value}/{{company}}/{{policy}}/{{timestamp}}.txt",
#                 connection="AZURE_STORAGE_CONNECTION_STRING")
# def create_summarizer_prompt(input_blob: func.InputStream, output_blob: func.Out[str]) -> None:
#     """Use language model to summarize diff."""
#     from src.summarizer import create_prompt, is_diff
#     blob = input_blob.read().decode()
#     if is_diff(blob):
#         prompt = create_prompt(blob)
#         output_blob.set(prompt)


# @http_wrap
# @app.blob_trigger(arg_name="input_blob", 
#                 path=f"documents/{Stage.PROMPT.value}/{{company}}/{{policy}}/{{timestamp}}.txt",
#                 connection="AZURE_STORAGE_CONNECTION_STRING")
# @app.durable_client_input(client_name="client")
# async def summarizer_blob_trigger(input_blob: func.InputStream, client: df.DurableOrchestrationClient):
#     """Blob trigger that starts the summarizer workflow orchestration."""
#     blob_name = input_blob.name.removeprefix("documents/")
#     orchestration_input = {
#         "task_id": blob_name,
#         "workflow_type": "summarizer"
#     }
#     await client.start_new("orchestrator", None, orchestration_input)
    

# @http_wrap
# @app.activity_trigger(input_name="input_data")
# def summarizer_processor(input_data: dict) -> str:
#     from src.summarizer import summarize
        
#     try:
#         blob_name = input_data['task_id']
#         prompt = load_text_blob(blob_name)
        
#         logger.debug(f"Summarizing {blob_name}")
#         summary_result = summarize(prompt)
        
#         in_path = parse_blob_path(blob_name)
#         out_path = f"{Stage.SUMMARY_RAW.value}/{in_path.company}/{in_path.policy}/{in_path.timestamp}.txt"
#         upload_text_blob(summary_result, out_path)
        
#         logger.info(f"Successfully summarized blob: {blob_name}")
#         return summary_result
        
#     except Exception as e:
#         blob_name = input_data.get('task_id', 'unknown')
#         logger.error(f"Error summarizing blob {blob_name}: {e}")
#         raise


# @http_wrap
# @app.blob_trigger(arg_name="input_blob", 
#                 path=f"documents/{Stage.SUMMARY_RAW.value}/{{company}}/{{policy}}/{{timestamp}}.txt",
#                 connection="AZURE_STORAGE_CONNECTION_STRING",
#                 data_type="string")
# @app.blob_output(arg_name="output_blob",
#                 path=f"documents/{Stage.SUMMARY_CLEAN.value}/{{company}}/{{policy}}/{{timestamp}}.json",
#                 connection="AZURE_STORAGE_CONNECTION_STRING")
# def parse_summary(input_blob: func.InputStream, output_blob: func.Out[str]) -> None:
#     from src.summarizer import parse_response_json
#     resp = parse_response_json(input_blob.read().decode())
#     output_blob.set(json.dumps(resp, indent=2))

@app.route("in_flight", auth_level=func.AuthLevel.FUNCTION)
@http_wrap
def list_in_flight(req: func.HttpRequest) -> func.HttpResponse:
    if "runtimeStatus" in req.params:
        query = req.params["runtimeStatus"]
        if query in df.OrchestrationRuntimeStatus._member_names_:
            params = {"runtimeStatus": req.params["runtimeStatus"]}
        else:
            return func.HttpResponse(f"Invalid parameter runtimeStatus={query}. " \
                                     f"Valid params are {df.OrchestrationRuntimeStatus._member_names_}",
                                      status_code=400, mimetype="plain/text")
    else:
        params={"runtimeStatus": ["Running", "Pending", "Suspended", "ContinuedAsNew"]}
    resp = requests.get("http://127.0.0.1:7071/runtime/webhooks/durabletask/instances", params)
    resp.raise_for_status()
    data = resp.json()
    formatted = dict(
        count = len(data),
        tasks = [dict(
            name = t.get('name'),
            status = t.get('runtimeStatus'),
            created = t.get('createdTime'),
            updated = t.get('lastUpdatedTime'),
            input_data = t.get('input'),
        ) for t in data]
    )
    return func.HttpResponse(json.dumps(formatted, indent=2), status_code=200, mimetype="application/json")
    
    
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
    return await check_cb(req, client)

@app.route(route="reset_circuit_breaker", auth_level=func.AuthLevel.FUNCTION)
@app.durable_client_input(client_name="client")
@pretty_error
async def reset_circuit_breaker(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    """Manually reset a breaker."""
    from src.circuit_breaker import reset_circuit_breaker as reset_cb
    return await reset_cb(req, client)