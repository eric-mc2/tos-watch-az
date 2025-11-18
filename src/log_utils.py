import logging
import os
from datetime import datetime
from app_utils import 

# Persist log path for lifespan of app
current_time = datetime.now().strftime("%d-%m-%Y-%H-%M-%S")
LOG_PATH = os.path.join('logs', f"app-{current_time}.log")

def setup_logger(name, loglvl = logging.INFO):
    log_fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    logging.basicConfig(level=loglvl, format=log_fmt)
    logger = logging.getLogger(name)
    logger.setLevel(loglvl)
    
    
    
    return logger

@dev_only
def _setup_logfile(logger, loglvl, log_fmt):
    # Add file handler to tee logs to shared file. But not in real life because it's not thread safe.
    os.makedirs('logs', exist_ok=True)
    file_handler = logging.FileHandler(LOG_PATH)
    file_handler.setLevel(loglvl)
    file_handler.setFormatter(logging.Formatter(log_fmt))
    logger.addHandler(file_handler)