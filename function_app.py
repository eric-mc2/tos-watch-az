import azure.functions as func
from pathlib import Path
from src.log_utils import setup_logger
from src.blob_utils import parse_blob_path

logger = setup_logger()

app = func.FunctionApp()


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


@app.blob_trigger(arg_name="input_blob", 
                path="documents/wayback-snapshots/{company}/{policy}/{timestamp}.html",
                connection="AZURE_STORAGE_CONNECTION_STRING")
@app.blob_output(arg_name="output_blob",
                path="documents/parsed/{company}/{policy}/{timestamp}.json",
                connection="AZURE_STORAGE_CONNECTION_STRING")
def parse_snap(input_blob: func.InputStream, output_blob: func.Out[str]) -> None:
    """Parse html snapshot into hierarchical doctree format."""
    from src.doctree import parse_html
    tree = parse_html(input_blob.read())
    output_blob.set(tree)


@app.blob_trigger(arg_name="input_blob", 
                path="documents/parsed/{company}/{policy}/{timestamp}.json",
                connection="AZURE_STORAGE_CONNECTION_STRING")
@app.blob_output(arg_name="output_blob",
                path="documents/annotated/{company}/{policy}/{timestamp}.json",
                connection="AZURE_STORAGE_CONNECTION_STRING")
def annotate_snap(input_blob: func.InputStream, output_blob: func.Out[str]) -> None:
    """Annotate doctree with corpus-level metadata."""
    from src.annotator import main as annotate_main
    path = parse_blob_path(input_blob.name)
    lines = annotate_main(path.company, path.policy, path.timestamp, input_blob.read())
    output_blob.set(lines)


@app.route(route="batch_diff", auth_level=func.AuthLevel.FUNCTION)
def batch_diff(req: func.HttpRequest) -> func.HttpResponse:
    from src.differ import diff_batch
    return _http_wrap(diff_batch, "batched diffs")


@app.route(route="single_diff", auth_level=func.AuthLevel.FUNCTION)
def single_diff(req: func.HttpRequest) -> func.HttpResponse:
    from src.differ import diff_single
    return _http_wrap(diff_single, "single diff", req.params['blob_name'])


@app.blob_trigger(arg_name="input_blob", 
                path="documents/diff/{company}/{policy}/{timestamp}.json",
                connection="AZURE_STORAGE_CONNECTION_STRING")
@app.blob_output(arg_name="output_blob",
                path="documents/prompts/{company}/{policy}/{timestamp}.txt",
                connection="AZURE_STORAGE_CONNECTION_STRING")
def summary_prompt(input_blob: func.InputStream, output_blob: func.Out[str]) -> None:
    """Use language model to summarize diff."""
    from src.summarizer import create_prompt, is_diff
    blob = input_blob.read()
    if is_diff(blob):
        prompt = create_prompt(blob)
        output_blob.set(prompt)

# @app.blob_trigger(arg_name="input_blob", 
#                 path="documents/prompts/{company}/{policy}/{timestamp}.json",
#                 connection="AZURE_STORAGE_CONNECTION_STRING")
# @app.blob_output(arg_name="output_blob",
#                 path="documents/summary/{company}/{policy}/{timestamp}.json",
#                 connection="AZURE_STORAGE_CONNECTION_STRING")
# def summarize_diff(input_blob: func.InputStream, output_blob: func.Out[str]) -> None:
#     """Use language model to summarize diff."""
#     from src.summarizer import summarize
#     summary = summarize(input_blob.read())
#     output_blob.set(summary)


def _http_wrap(task, taskname, *args, **kwargs):
    logger.info(f"Starting {task}")
    try:
        task(*args, **kwargs)  
        return func.HttpResponse(f"Successfully processed {taskname}", status_code=200)
    except Exception as e:
        logger.error(f"Error processing {taskname}: {e}")
        return func.HttpResponse(f"Error processing {taskname}: {str(e)}", status_code=500)