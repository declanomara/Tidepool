import configparser
import logging
import multiprocessing
import sys

from . import OANDA
from functools import wraps


def load_config(file='cfg.ini'):
    parser = configparser.ConfigParser()
    parser.read(file)

    cfg = {
        'token': parser['DataGatherer']['token'],
        'alias': parser['DataGatherer']['alias'],
        'db_address': parser['DataGatherer']['db_address'],
        'db_username': parser['DataGatherer']['db_username'],
        'db_password': parser['DataGatherer']['db_password'],
        'db_string': parser['DataGatherer']['db_string']
    }

    return cfg


def seconds_to_human(s):
    if s == 0:
        return '0s'

    M = 60
    H = M * 60
    D = H * 24
    W = D * 7

    w = s // W
    s = s - (w * W)
    d = s // D
    s = s - (d * D)
    h = s // H
    s = s - (h * H)
    m = s // M
    s = s - (m * M)

    times = {'w': w, 'd': d, 'h': h, 'm': m, 's': s}
    parts = [f'{val}{key}' for key, val in times.items() if val > 0]
    return ' '.join(parts)


def ignore_keyboard_interrupt(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)

        except KeyboardInterrupt:
            return
    return wrapper


def moving_average(previous, new, count):
    return previous * (count / (count + 1)) + new * (1 / (count + 1))

def create_logger():
    logger = multiprocessing.get_logger()
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "[%(asctime)s| %(levelname)s| %(processName)s] %(message)s"
    )
    handler = logging.FileHandler("logs/latest.log")
    handler.setFormatter(formatter)

    if len(logger.handlers) < 1:
        logger.addHandler(handler)

    out_handler = logging.StreamHandler(sys.stdout)
    out_handler.setFormatter(formatter)
    out_handler.setLevel(logging.WARNING)

    if len(logger.handlers) < 2:
        logger.addHandler(out_handler)

    return logger
