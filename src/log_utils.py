import logging

def setup_logger(name, loglvl = logging.INFO):
    log_fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    logging.basicConfig(level=loglvl, format=log_fmt)
    logger = logging.getLogger(name)
    logger.setLevel(loglvl)
    
    # Add file handler to tee logs to shared file
    file_handler = logging.FileHandler('app.log')
    file_handler.setLevel(loglvl)
    file_handler.setFormatter(logging.Formatter(log_fmt))
    logger.addHandler(file_handler)
    
    return logger
