
import json
import logging
from typing import Generator
import azure.functions as func
from azure import durable_functions as df
from azure.functions.decorators.core import DataType

from schemas.summary.v0 import MODULE as SUMMARY_MODULE
from schemas.factcheck.v0 import MODULE as FACTCHECK_MODULE
from schemas.claim.v0 import MODULE as CLAIMS_MODULE
from schemas.judge.v0 import MODULE as JUDGE_MODULE
from src.transforms.seeds import STATIC_URLS
from src.transforms.llm_transform import create_llm_activity_processor, create_llm_parser
from src.utils.log_utils import setup_logger
from src.utils.app_utils import http_wrap, pretty_error, load_env_vars
from src.stages import Stage
from src.orchestration.orchestrator import OrchData
from src.container import ServiceContainer

load_env_vars()

app = func.FunctionApp()

logger = setup_logger(__name__, logging.DEBUG)
logging.getLogger('azure').setLevel(logging.WARNING)

container = ServiceContainer.create()

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
        data = [await check_cb(workflow_type, client)]
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

@app.activity_trigger(input_name="input_data")
@pretty_error(retryable=True)
def meta_processor(input_data: dict) -> None:
    container.wayback_transform.scrape_wayback_metadata(input_data['task_id'], input_data['company'])


@app.blob_trigger(arg_name="input_blob",
                path="documents/01-metadata/{company}/{policy}/metadata.json",
                connection=container.storage.adapter.get_connection_key())
@app.durable_client_input(client_name="client")
@pretty_error
async def scraper_blob_trigger(input_blob: func.InputStream,
                               client: df.DurableOrchestrationClient) -> None:
    """Blob trigger that starts the scraper workflow orchestration."""
    parts = container.storage.parse_blob_path(input_blob.name)

    # Parse and re-save metadata
    metadata = container.wayback_transform.parse_wayback_metadata(parts.company, parts.policy)

    # Sample metadata for seeding initial db
    metadata = container.wayback_transform.sample_wayback_metadata(metadata, parts.company, parts.policy)

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
def scraper_processor(input_data: dict) -> None:
    snap_url = input_data['task_id']
    company = input_data['company']
    policy = input_data['policy']
    timestamp = input_data['timestamp']
    container.snapshot_transform.get_wayback_snapshot(company, policy, timestamp, snap_url)
    logger.info(f"Successfully scraped {snap_url}")


@app.timer_trigger(arg_name="input_timer",
                   schedule="0 0 * * 1")
@app.durable_client_input(client_name="client")
@pretty_error
async def scraper_scheduled_trigger(input_timer: func.TimerRequest,
                              client: df.DurableOrchestrationClient) -> None:
    from src.utils.path_utils import extract_policy
    import time
    urls = STATIC_URLS
    for company, url_list in urls.items():
        for url in url_list:
            policy = extract_policy(url)
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
    url = input_data['task_id']
    company = input_data['company']
    policy = input_data['policy']
    timestamp = input_data['timestamp']
    container.snapshot_transform.get_website(company, policy, timestamp, url)
    logger.info(f"Successfully scraped {company}/{policy}")


@app.blob_trigger(arg_name="input_blob",
                path="documents/02-snapshots/{company}/{policy}/{timestamp}.html",
                connection=container.storage.adapter.get_connection_key(),
                data_type=DataType.STRING)
@app.blob_output(arg_name="output_blob",
                path="documents/03-doctrees/{company}/{policy}/{timestamp}.json",
                connection=container.storage.adapter.get_connection_key())
@pretty_error
def parse_snap(input_blob: func.InputStream, output_blob: func.Out[str]) -> None:
    """Parse html snapshot into hierarchical doctree format."""
    from src.transforms.doctree import parse_html
    tree = parse_html(input_blob.read().decode())
    output_blob.set(tree.__repr__())


@app.blob_trigger(arg_name="input_blob",
                path="documents/03-doctrees/{company}/{policy}/{timestamp}.json",
                connection=container.storage.adapter.get_connection_key(),
                data_type=DataType.STRING)
@app.blob_output(arg_name="output_blob",
                path="documents/04-doclines/{company}/{policy}/{timestamp}.json",
                connection=container.storage.adapter.get_connection_key())
@pretty_error
def annotate_snap(input_blob: func.InputStream, output_blob: func.Out[str]) -> None:
    """Annotate doctree with corpus-level metadata."""
    from src.transforms.annotator import annotate_and_pool
    path = container.storage.parse_blob_path(input_blob.name)
    lines = annotate_and_pool(path.company, path.policy, path.timestamp, input_blob.read().decode())
    output_blob.set(lines)


@app.blob_trigger(arg_name="input_blob",
                path="documents/04-doclines/{company}/{policy}/{timestamp}.json",
                connection=container.storage.adapter.get_connection_key())
@pretty_error
def single_diff(input_blob: func.InputStream) -> None:
    container.differ_transform.diff_and_save(input_blob.name)

@app.blob_trigger(arg_name="input_blob",
                path="documents/05-diffs-raw/{company}/{policy}/{timestamp}.json",
                connection=container.storage.adapter.get_connection_key(),
                data_type=DataType.STRING)
@app.blob_output(arg_name="output_blob",
                path="documents/05-diffs-clean/{company}/{policy}/{timestamp}.json",
                connection=container.storage.adapter.get_connection_key())
@pretty_error
def clean_diffs(input_blob: func.InputStream, output_blob: func.Out[str]) -> None:
    blob = input_blob.read().decode()
    if container.differ_transform.has_diff(blob):
        diff = container.differ_transform.clean_diff(blob)
        output_blob.set(diff.model_dump_json())


@app.blob_trigger(arg_name="input_blob",
                path="documents/05-diffs-clean/{company}/{policy}/{timestamp}.json",
                connection=container.storage.adapter.get_connection_key())
@app.durable_client_input(client_name="client")
@pretty_error
async def summarizer_blob_trigger(input_blob: func.InputStream, client: df.DurableOrchestrationClient) -> None:
    """Blob trigger that starts the summarizer workflow orchestration."""
    parts = container.storage.parse_blob_path(input_blob.name)
    blob_name = container.storage.unparse_blob_path(parts)
    orchestration_input = OrchData(blob_name, "summarizer", parts.company, parts.policy, parts.timestamp).to_dict()
    logger.info(f"Initiating orchestration for {blob_name}")
    await client.start_new("orchestrator", None, orchestration_input)


@app.activity_trigger(input_name="input_data")
@pretty_error(retryable=True)
def summarizer_processor(input_data: dict) -> None:
    processor = create_llm_activity_processor(container.storage,
                                              container.summarizer_transform.summarize,
                                              Stage.SUMMARY_RAW.value,
                                              "summarizer")
    return processor(input_data)


@app.blob_trigger(arg_name="input_blob",
                path="documents/07-summary-raw/{company}/{policy}/{timestamp}/latest.txt",
                connection=container.storage.adapter.get_connection_key(),
                data_type=DataType.STRING)
@pretty_error
def parse_summary(input_blob: func.InputStream) -> None:
    # XXX: There is a race condition here IF you fan out across versions. Would need new orchestrator for updating latest.
    parser = create_llm_parser(container.storage, container.summarizer_transform.llm, SUMMARY_MODULE, Stage.SUMMARY_CLEAN.value)
    return parser(input_blob)


# Claim Extraction Pipeline
@app.blob_trigger(arg_name="input_blob",
                path="documents/08-summary-clean/{company}/{policy}/{timestamp}/latest.json",
                connection=container.storage.adapter.get_connection_key())
@app.durable_client_input(client_name="client")
@pretty_error
async def claim_extractor_blob_trigger(input_blob: func.InputStream, client: df.DurableOrchestrationClient) -> None:
    """Blob trigger that starts the claim extractor workflow orchestration."""
    parts = container.storage.parse_blob_path(input_blob.name)
    blob_name = container.storage.unparse_blob_path(parts)
    orchestration_input = OrchData(blob_name, "claim_extractor", parts.company, parts.policy, parts.timestamp).to_dict()
    logger.info(f"Initiating claim extraction orchestration for {blob_name}")
    await client.start_new("orchestrator", None, orchestration_input)


@app.activity_trigger(input_name="input_data")
@pretty_error(retryable=True)
def claim_extractor_processor(input_data: dict) -> None:
    processor = create_llm_activity_processor(container.storage,
                                              container.claim_extractor_transform.extract_claims,
                                              Stage.CLAIM_RAW.value,
                                              "claim_extractor")
    return processor(input_data)


@app.blob_trigger(arg_name="input_blob",
                path="documents/10-claim-raw/{company}/{policy}/{timestamp}/latest.txt",
                connection=container.storage.adapter.get_connection_key(),
                data_type=DataType.STRING)
@pretty_error
def parse_claims(input_blob: func.InputStream) -> None:
    parser = create_llm_parser(container.storage,
                               container.claim_extractor_transform.llm,
                               CLAIMS_MODULE,
                               Stage.CLAIM_CLEAN.value)
    return parser(input_blob)


# Claim Checking Pipeline
@app.blob_trigger(arg_name="input_blob",
                path="documents/11-claim-clean/{company}/{policy}/{timestamp}/latest.json",
                connection=container.storage.adapter.get_connection_key())
@app.durable_client_input(client_name="client")
@pretty_error
async def claim_checker_blob_trigger(input_blob: func.InputStream, client: df.DurableOrchestrationClient) -> None:
    """Blob trigger that starts the claim checker workflow orchestration."""
    parts = container.storage.parse_blob_path(input_blob.name)
    blob_name = container.storage.unparse_blob_path(parts)
    orchestration_input = OrchData(blob_name, "claim_checker", parts.company, parts.policy, parts.timestamp).to_dict()
    logger.info(f"Initiating claim checking orchestration for {blob_name}")
    await client.start_new("orchestrator", None, orchestration_input)


@app.activity_trigger(input_name="input_data")
@pretty_error(retryable=True)
def claim_checker_processor(input_data: dict) -> None:
    processor = create_llm_activity_processor(container.storage,
                                              container.claim_checker_transform.check_claim,
                                              Stage.FACTCHECK_RAW.value,
                                              "claim_checker",
                                              paired_input_stage=Stage.DIFF_CLEAN.value)
    return processor(input_data)


@app.blob_trigger(arg_name="input_blob",
                path="documents/12-factcheck-raw/{company}/{policy}/{timestamp}/latest.txt",
                connection=container.storage.adapter.get_connection_key(),
                data_type=DataType.STRING)
@pretty_error
def parse_factcheck(input_blob: func.InputStream) -> None:
    parser = create_llm_parser(container.storage,
                               container.claim_checker_transform.llm,
                               FACTCHECK_MODULE,
                               Stage.FACTCHECK_CLEAN.value)
    return parser(input_blob)


# Judge Pipeline
@app.blob_trigger(arg_name="input_blob",
                path="documents/13-factcheck-clean/{company}/{policy}/{timestamp}/latest.json",
                connection=container.storage.adapter.get_connection_key())
@app.durable_client_input(client_name="client")
@pretty_error
async def judge_blob_trigger(input_blob: func.InputStream, client: df.DurableOrchestrationClient) -> None:
    """Blob trigger that starts the judge workflow orchestration."""
    parts = container.storage.parse_blob_path(input_blob.name)
    blob_name = container.storage.unparse_blob_path(parts)
    orchestration_input = OrchData(blob_name, "judge", parts.company, parts.policy, parts.timestamp).to_dict()
    logger.info(f"Initiating judge orchestration for {blob_name}")
    await client.start_new("orchestrator", None, orchestration_input)


@app.activity_trigger(input_name="input_data")
@pretty_error(retryable=True)
def judge_processor(input_data: dict) -> None:
    # Judge needs summary blob from earlier stage
    processor = create_llm_activity_processor(container.storage,
                                              container.judge_transform.judge,
                                              Stage.JUDGE_RAW.value,
                                              "judge",
                                              paired_input_stage=Stage.SUMMARY_CLEAN.value)
    return processor(input_data)


@app.blob_trigger(arg_name="input_blob",
                path="documents/14-judge-raw/{company}/{policy}/{timestamp}/latest.txt",
                connection=container.storage.adapter.get_connection_key(),
                data_type=DataType.STRING)
@pretty_error
def parse_judge(input_blob: func.InputStream) -> None:
    parser = create_llm_parser(container.storage,
                               container.judge_transform.llm,
                               JUDGE_MODULE,
                               Stage.JUDGE_CLEAN.value)
    return parser(input_blob)


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


