
import logging
from src.log_utils import setup_logger
import azure.functions as func
import json
import traceback
from functools import wraps

logger = setup_logger(__name__, logging.DEBUG)

def http_wrap(app_func):
    """
    Decorator that wraps a function to provide logging and HttpResponse handling.
    """
    @wraps(app_func)
    def wrapper(*args, **kwargs):
        try:
            result = app_func(*args, **kwargs)
            if isinstance(result, func.HttpResponse):
                return result
            return func.HttpResponse(f"Successfully processed {app_func.__name__}", status_code=200)
        except Exception as e:
            formatted = {
                "app": {app_func.__name__},
                "type": type(e).__name__,
                "message": str(e),
                "traceback": traceback.format_exc()
            }
            error_msg = json.dumps(formatted, indent=2)
            logger.error(error_msg)
            return func.HttpResponse(error_msg, mimetype="application/json", status_code=500)
    return wrapper


def pretty_error(app_func):
    """
    Decorator that wraps a function to provide logging handling.
    """
    @wraps(app_func)
    def wrapper(*args, **kwargs):
        try:
            return app_func(*args, **kwargs)
        except Exception as e:
            formatted = {
                "app": {app_func.__name__},
                "type": type(e).__name__,
                "message": str(e),
                "traceback": traceback.format_exc()
            }
            error_msg = json.dumps(formatted, indent=2)
            logger.error(error_msg)
            raise Exception(error_msg)
    return wrapper