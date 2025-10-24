import azure.functions as func
import logging
from azure.storage.blob import BlobClient
from src.log_utils import setup_logger

logger = setup_logger(logging.INFO)

def main(input_blob: func.InputStream) -> None:
    pass