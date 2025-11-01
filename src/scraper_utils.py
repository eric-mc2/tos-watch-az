import logging
from src.blob_utils import load_json_blob
from src.log_utils import setup_logger
from pathlib import Path

logger = setup_logger(__name__, logging.INFO)


def validate_url(url):
    """Validate URL format"""
    from urllib.parse import urlparse
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def sanitize_urlpath(url):
    # Parse URL for file structure
    from urllib.parse import urlparse
    from pathlib import Path
    parsed_url = urlparse(url)
    url_path = parsed_url.path if parsed_url.path not in ['','/'] else parsed_url.netloc
    url_path = Path(url_path).parts[-1] or 'index'
    url_path = sanitize_path_component(url_path)
    return url_path


def sanitize_path_component(path_component):
    """Sanitize a path component for use in blob names"""
    import re
    # Replace invalid characters with underscores
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', path_component)
    # Remove any leading/trailing whitespace and dots
    sanitized = sanitized.strip(' .')
    # Remove file extension
    # Note we don't want to move domain names.
    for ext in [".html"]:
        sanitized = sanitized.removesuffix(ext)
    # Ensure it's not empty
    return sanitized if sanitized else 'default'


def load_urls(input_container_name, input_blob_name):
     # Download the URLs file from blob storage
     # Validate URL
    urls = load_json_blob(input_container_name, input_blob_name)
    for company, url_list in urls.items():
        for url in url_list:
            if not validate_url(url):
                raise ValueError(f"Invalid URL format: {url}")
    return urls

