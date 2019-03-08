import logging


def get_logger(name):
    logger = logging.getLogger(name)
    handler = logging.StreamHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger
