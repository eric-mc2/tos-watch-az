import logging

def setup_logger(loglvl = logging.INFO):
    log_fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    logging.basicConfig(level=logging.WARNING, format=log_fmt)
    logger = logging.getLogger(__name__)
    logger.setLevel(loglvl)
    return logger
