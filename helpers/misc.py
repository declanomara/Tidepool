import configparser
import logging
import multiprocessing
import sys

from functools import wraps
from logging.handlers import RotatingFileHandler
from typing import Callable, Union


def load_config(file: str = "cfg.ini") -> dict:
    """
    :param file: Path to a file containing config in .ini format.
    :return: A dictionary containing all config variables.
    :rtype: dict
    """
    parser = configparser.ConfigParser()
    parser.read(file)

    cfg = {
        "token": parser["DataGatherer"]["token"],
        "alias": parser["DataGatherer"]["alias"],
        "db_address": parser["DataGatherer"]["db_address"],
        "db_username": parser["DataGatherer"]["db_username"],
        "db_password": parser["DataGatherer"]["db_password"],
        "db_string": parser["DataGatherer"]["db_string"],
        "live": parser.getboolean("DataGatherer", "live"),
    }

    return cfg


def seconds_to_human(s: float) -> str:
    """
    :param s: Number of seconds to convert to human readable format
    :return: A string representation of the number of seconds in a human readable format
    :rtype: str
    """
    if s == 0:
        return "0s"

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

    times = {"w": w, "d": d, "h": h, "m": m, "s": s}
    parts = [f"{val}{key}" for key, val in times.items() if val > 0]
    return " ".join(parts)


def seconds_to_us(seconds: float) -> int:
    return int(seconds * 10 ** 6)


def ignore_keyboard_interrupt(func: Callable) -> Callable:
    """
    :param func: Callable object to wrap with KeyboardInterrupt ignore
    :return: Returns callable object func, with additional functionality to ignore KeyboardInterrupt
    :rtype: Callable object that exits cleanly in case of KeyboardInterrupt
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)

        except KeyboardInterrupt:
            return

    return wrapper


def moving_average(previous: Union[int, float], new: Union[int, float], count: int) -> Union[int, float]:
    """
    :param previous: Previous value of moving average
    :param new: New value of data for which to average
    :param count: Count of data points in average
    :return: Returns moving average as float or int
    :rtype: Union[int, float]
    """
    return previous * (count / (count + 1)) + new * (1 / (count + 1))


def create_logger() -> logging.Logger:
    """
    :return: Returns a logger object
    :rtype: logging.Logger
    """
    logger = multiprocessing.get_logger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "[%(asctime)s| %(levelname)s| %(processName)s] %(message)s"
    )
    handler = RotatingFileHandler("logs/log.log", mode='a', maxBytes=100_000_000, backupCount=3)
    handler.setFormatter(formatter)

    if len(logger.handlers) < 1:
        logger.addHandler(handler)

    out_handler = logging.StreamHandler(sys.stdout)
    out_handler.setFormatter(formatter)
    out_handler.setLevel(logging.WARNING)

    if len(logger.handlers) < 2:
        logger.addHandler(out_handler)

    return logger

