import logging
import sys
from typing import Any

def get_handler(textio: Any):
    handler = logging.StreamHandler(textio)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    return handler

def get_logger(name: str, textio: Any):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if not logger.hasHandlers():  # Only add handler if none exist
        logger.addHandler(get_handler(textio=textio))
        logger.propagate = False  # Prevent propagation to root logger

    return logger

class TasksLogger():
    def __init__(self, logPrefix=""):
        self.logger = get_logger(__name__, sys.stdout)
        self.logPrefix = logPrefix + " - " if logPrefix else ""

    def debug(self, msg):
        self.logger.debug(self.logPrefix + msg)

    def info(self, msg):
        self.logger.info(self.logPrefix + msg)

    def error(self, msg):
        self.logger.error(self.logPrefix + msg)

    def setLoggerLevel(self, level: str):
        if level:
            self.logger.setLevel(level)
        else:
            self.logger.setLevel('NOTSET')
