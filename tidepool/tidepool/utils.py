import logging

from pathlib import Path
from multiprocessing import Queue
from logging.handlers import QueueHandler, QueueListener


def create_logger(log_file: Path, logging_queue: Queue = None, log_level: int = logging.INFO):
    # Create a file handler that logs to a file
    if not log_file.parent.exists():
        log_file.parent.mkdir(parents=True)
        
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)

    # Create a stream handler that logs to stdout
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(log_level)

    # Create a formatter and add it to the handlers
    formatter = logging.Formatter('[%(asctime)s][%(name)s][%(levelname)s] %(message)s',datefmt="%Y-%m-%d %H:%M:%S")
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    # Create a logger for the main module
    logger = logging.getLogger()
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.setLevel(logging.INFO)

    # If a logging queue is provided, add a listener to it.
    if logging_queue is not None:
        listener = QueueListener(logging_queue, file_handler, stream_handler)
        listener.start()

def get_multiprocessing_logger(name: str, logging_queue: Queue, level: int = logging.INFO):
        if logging_queue is not None:
            h = QueueHandler(logging_queue)  # Just the one handler needed
            root = logging.getLogger()
            root.addHandler(h)
            root.setLevel(level)

        return logging.getLogger(f'__main__.{name}')