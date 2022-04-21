import configparser

from . import OANDA


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

