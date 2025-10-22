import logging
import json
import azure.functions as func
from shared.blob_utils import get_blob_service_client, ensure_container

# Configure logging at module level for Azure Functions
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def seed():
    logging.info('Seeding ToS urls.')
    with open('seed_urls/static_urls.json', 'r') as f:
        urls = f.read()
    blob_service_client = get_blob_service_client()
    ensure_container('documents')
    blob_client = blob_service_client.get_blob_client(
                container='documents', 
                blob='static_urls.json'
            )
    blob_client.upload_blob(urls, overwrite=True)
            
def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        seed()
        return func.HttpResponse(
            f"Successfully seeded urls",
            status_code=200
        )
        
    except Exception as e:
        logging.error(f"Error seeding urls: {e}")
        return func.HttpResponse(
            f"Error seeding urls: {str(e)}",
            status_code=500
        )
