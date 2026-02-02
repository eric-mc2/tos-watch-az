
import azure.functions as func
import logging
import json
import traceback
import os
from functools import wraps
from dataclasses import dataclass, asdict
from src.utils.log_utils import setup_logger

logger = setup_logger(__name__, logging.DEBUG)

@dataclass
class AppError:
    app: str
    error_type: str
    message: str
    traceback: str

    def __str__(self):
        return json.dumps(self.to_dict(), indent=2)
    def to_dict(self):
        return asdict(self)

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
            app_error = AppError(
                app = app_func.__name__,
                error_type = type(e).__name__,
                message = str(e),
                traceback = condensed_tb(e).splitlines()
            )
            logger.error(app_error)
            return func.HttpResponse(str(app_error), mimetype="application/json", status_code=500)
    return wrapper


def pretty_error(func_arg=None, retryable=False):
    """
    Decorator that wraps a function to provide logging handling.
    """
    def decorator(app_func):
        @wraps(app_func)
        def wrapper(*args, **kwargs):
            try:
                return app_func(*args, **kwargs)
            except Exception as e:
                app_error = AppError(
                    app = app_func.__name__,
                    error_type = type(e).__name__,
                    message = str(e),  # <-- doesn't include stacktrace
                    traceback = condensed_tb(e).splitlines()  # <-- now the stacktrace
                )
                if not retryable:
                    logger.error(app_error)
                # Azure will wrap this error in its own C# error and then shove it back
                # into a Python exception message which is impossible to parse.
                # So instead of raising, always return a meaningful value from top level functions.
                # raise type(e)(error_msg) from e
                return app_error.to_dict()
        return wrapper
    if func_arg is None:
        return decorator
    else:
        return decorator(func_arg)


def condensed_tb(exc) -> str:
    """
    Formats a traceback object into a condensed list of strings, showing only
    the file basename, line number, and function name, significantly
    reducing verbosity by stripping full directory paths.
    """
    condensed_trace = []
    # Extract frame data from the traceback object
    frames = traceback.extract_tb(exc.__traceback__)
    
    for frame in frames:
        # Use os.path.basename to strip the full directory path
        filename = os.path.basename(frame.filename)
        # Format: [filename:line_num] in function_name
        condensed_trace.append(
            f'{filename}:{frame.lineno} in {frame.name}'
        )
    return "\n".join(condensed_trace)
