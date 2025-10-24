import logging
import azure.functions as func
from src.blob_utils import ensure_container, upload_json_blob
from src.log_utils import setup_logger

logger = setup_logger(logging.INFO)
            
def main(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('Seeding ToS urls.')
    with open('data/static_urls.json') as f:
        urls = f.read()

    try:
        ensure_container('documents')
        upload_json_blob(urls, 'documents', 'static_urls.json')
        return func.HttpResponse(f"Successfully seeded urls", status_code=200) 
    except Exception as e:
        logger.error(f"Error seeding urls: {e}")
        return func.HttpResponse(f"Error seeding urls: {e}", status_code=500)
