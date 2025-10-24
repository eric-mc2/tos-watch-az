import azure.functions as func
from pathlib import Path
from src.log_utils import setup_logger

logger = setup_logger()

app = func.FunctionApp()

@app.route(route="seed_urls", auth_level=func.AuthLevel.FUNCTION)
def seed_urls(req: func.HttpRequest) -> func.HttpResponse:
    """Post seed URLs to blob storage for scraping"""
    from src.seeder import main as seed_main
    return seed_main(req)

@app.route(route="scrape", auth_level=func.AuthLevel.FUNCTION)
def scrape_snaps(req: func.HttpRequest) -> func.HttpResponse:
    """Collect wayback snapshots from static URL list"""
    from src.scraper import main as scraper_main
    return scraper_main(req)

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
    blob_path = Path(input_blob.name)
    company = blob_path.parts[2]
    policy = blob_path.parts[3]
    ts = blob_path.parts[4]
    lines = annotate_main(company, policy, ts, input_blob.read())
    output_blob.set(lines)

# @app.blob_trigger(arg_name="input_blob", 
#                 path="documents/parsed-html/{name}",
#                 connection="AZURE_STORAGE_CONNECTION_STRING")
# def diff_snap(input_blob: func.InputStream) -> None:
#     from src.differ import main as diff_main
#     diff_main(input_blob)