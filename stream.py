import configparser
import requests
import json
import pymongo
import multiprocessing

from pprint import pprint
from moonwrapper import gather_account_id, gather_acct_instruments, MoonQueue


STREAM_API = 'https://stream-fxpractice.oanda.com/'
MAX_QUEUE_LENGTH = 70


def monitor_queue(q: MoonQueue):
    while True:
        s = q.qsize()
        if s>MAX_QUEUE_LENGTH:
            print(f'Current queue length: {s}')


def record_data(q: MoonQueue):
    # Configure DB
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    crystalmoondb = client['crystalmoon']
    collection = crystalmoondb['raw']

    while True:
        data_point = q.get()
        collection.insert_one(data_point)


def stream_prices(token,  instruments, q: MoonQueue):
    headers = {'Authorization': f'Bearer {token}'}
    url = f'{STREAM_API}/v3/accounts/{id}/pricing/stream?instruments={"".join([instrument + "," for instrument in instruments])}'

    data_points = 0
    with requests.get(url, headers=headers, stream=True) as resp:
        for line in resp.iter_lines():
            if line:
                data_points += 1
                data_point = json.loads(line.decode('utf-8'))
                q.put(data_point)

            if data_points % 100 == 0:
                print(f'Gathered {data_points} new data points...')


if __name__ == '__main__':
    # Configure API
    config = configparser.ConfigParser()
    config.read('config.ini')
    token = config['primary']['API_TOKEN']
    alias = 'crystalmoon'  # Main account for trading
    id = gather_account_id(alias, token)

    instruments = gather_acct_instruments(id, token)

    # Script header
    print('=' * 30)
    print(f'Streaming {len(instruments)} instruments:')
    pprint(instruments)
    print('='*30)

    q = MoonQueue()
    processes = list()
    processes.append(multiprocessing.Process(target=stream_prices, args=(token, instruments, q)))
    processes.append(multiprocessing.Process(target=record_data, args=(q,)))
    processes.append(multiprocessing.Process(target=monitor_queue, args=(q,)))

    for p in processes:
        p.start()

    for p in processes:
        p.join()