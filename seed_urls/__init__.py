import logging
import azure.functions as func
from shared.blob_utils import get_blob_service_client, ensure_container, upload_json_blob

# Configure logging at module level for Azure Functions
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
            
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Seeding ToS urls.')
    with open('seed_urls/static_urls.json') as f:
        urls = f.read()

    try:
        ensure_container('documents')
        upload_json_blob(urls, 'documents', 'static_urls.json')
        return func.HttpResponse(f"Successfully seeded urls", status_code=200) 
    except Exception as e:
        logging.error(f"Error seeding urls: {e}")
        return func.HttpResponse(f"Error seeding urls: {e}", status_code=500)
