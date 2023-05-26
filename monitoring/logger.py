import logging


def create_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    handler = logging.FileHandler('monitoring/logs/words_counter.log')
    handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s - %(extra)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger


class Logger:

    def __init__(self):
        self.logger = create_logger()


