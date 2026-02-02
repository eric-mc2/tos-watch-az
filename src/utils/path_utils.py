import logging
from src.utils.log_utils import setup_logger
from validators import url as is_valid
from validators import ValidationError

logger = setup_logger(__name__, logging.INFO)


def validate_url(url):
    """Validate URL format"""
    try:
        return is_valid(url)
    except ValidationError as e:
        return False
    

def extract_policy(url):
    # Parse URL for file structure
    from urllib.parse import urlparse
    from pathlib import Path
    parsed_url = urlparse(url)
    url_path = parsed_url.path if parsed_url.path not in ['','/'] else parsed_url.netloc
    url_path = Path(url_path).parts[-1] or 'index'
    url_path = _sanitize_path_component(url_path)
    return url_path


def _sanitize_path_component(path_component):
    """Sanitize a path component for use in blob names"""
    import re
    # Replace invalid characters with underscores
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', path_component)
    # Remove any leading/trailing whitespace and dots
    sanitized = sanitized.strip(' .')
    # Remove www
    sanitized = sanitized.removeprefix("www.")
    # Remove file extension
    # Note we don't want to move domain names.
    for ext in [".html"]:
        sanitized = sanitized.removesuffix(ext)
    # Ensure it's not empty
    return sanitized if sanitized else 'default'
