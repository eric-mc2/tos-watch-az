import logging

def setup_logger(name, loglvl = logging.INFO):
    log_fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    logging.basicConfig(level=loglvl, format=log_fmt)
    logger = logging.getLogger(name)
    logger.setLevel(loglvl)
    return logger
