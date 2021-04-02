import time
import dateutil.parser as dp
import logging
import sys
import pymongo

from pathlib import Path


def ISO8601_to_epoch(timestamp: str):
    return dp.parse(timestamp).timestamp()


if __name__ == '__main__':
    # Configure logging
    Path('logs').mkdir(exist_ok=True)
    log = f'logs/{time.strftime("%Y%m%d-%H%M%S")}-processing.log'
    logging.basicConfig(filename=log, format='%(levelname)s:%(message)s', level=logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s]: %(message)s', "%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    logging.getLogger().addHandler(handler)

    # Fun stuff
    db = 'mongodb://3.22.74.49:27017/'
    client = pymongo.MongoClient(db)
    logging.info('Connection to database established')
    tidepooldb = client['tidepool']

    with tidepooldb['raw'].watch([{'$match': {'operationType': 'insert'}}]) as stream:
        for insert_change in stream:
            raw_data = insert_change['fullDocument']
            if raw_data['type'] != 'PRICE':
                continue

            instrument = raw_data['instrument']
            data = {
                'bid': float(raw_data['closeoutBid']),
                'ask': float(raw_data['closeoutAsk']),
                'time': ISO8601_to_epoch(raw_data['time'])

            }

            tidepooldb[instrument].insert_one(data)
            logging.debug(f'{instrument} data point processed')

