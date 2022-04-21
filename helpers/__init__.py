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
