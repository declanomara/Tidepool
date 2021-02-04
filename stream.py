import configparser
import requests
import json
import time
import pymongo
import multiprocessing

from moonwrapper import gather_account_id, gather_acct_instruments, MoonQueue


STREAM_URL = 'https://stream-fxpractice.oanda.com/'
MAX_QUEUE_LENGTH = 30
MAX_RECORDING_PROCESSES = 10
RESTART_INTERVAL = 5*60


def dispatch_processes(token, instruments):
    while True:
        q = MoonQueue()
        recording_processes = list()
        recording_processes.append(multiprocessing.Process(target=record_data, args=(q,)))

        gathering_process = multiprocessing.Process(target=stream_prices, args=(token, instruments, q))
        monitoring_process = multiprocessing.Process(target=monitor_queue, args=(q, recording_processes))

        for p in [*recording_processes, gathering_process, monitoring_process]:
            p.start()

        time.sleep(RESTART_INTERVAL) # Restart script every 5 hours

        print('Restarting script...')

        for p in [*recording_processes, gathering_process, monitoring_process]:
            p.terminate()

        del q
        del recording_processes
        del gathering_process
        del monitoring_process


def monitor_queue(q: MoonQueue, recording_processes):
    tick_count = 0
    while True:
        tick_count += 1
        # Monitor queue size and adjust recording pool accordingly
        s = q.qsize()
        if s > MAX_QUEUE_LENGTH:
            print(f'Max queue length exceeded, current length: {s}')

            if len(recording_processes) < MAX_RECORDING_PROCESSES:
                # Spawn new process to handle excess load
                p = multiprocessing.Process(target=record_data, args=(q,))
                p.start()
                recording_processes.append(p)
                print(f'Spawned new data recording process. Process count: {len(recording_processes)}')

        if tick_count % 1000 == 0:  # Don't want to do this too often
            if len(recording_processes) >= MAX_RECORDING_PROCESSES:
                print(f'Tick count: {tick_count}')
                # Replace oldest process in case recording is broken for some reason
                p = multiprocessing.Process(target=record_data, args=(q,))
                p.start()
                recording_processes.append(p)

                recording_processes.pop(0).terminate()
                print(f'Already at max processes, recycling a process. Process count: {len(recording_processes)}')



        if s < MAX_QUEUE_LENGTH and len(recording_processes) > 1:
            recording_processes.pop(0).terminate()
            print(f'Killed data recording process. Process count: {len(recording_processes)}')


def record_data(q: MoonQueue):
    # Configure DB
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    crystalmoondb = client['crystalmoon']
    collection = crystalmoondb['raw']

    while True:
        try:
            data_point = q.get()
            collection.insert_one(data_point)

        except Exception as e:
            print(e)


def stream_prices(token,  instruments, q: MoonQueue):
    headers = {'Authorization': f'Bearer {token}'}
    url = f'{STREAM_URL}/v3/accounts/{id}/pricing/stream?instruments={"".join([instrument + "," for instrument in instruments])}'

    data_points = 0

    while True:
        try:
            with requests.get(url, headers=headers, stream=True) as resp:
                for line in resp.iter_lines():
                    if line:
                        data_points += 1
                        data_point = json.loads(line.decode('utf-8'))
                        q.put(data_point)

                    if data_points % 100 == 0:
                        print(f'Gathered {data_points} new data points...')

        except Exception as e:
            print(f'API connection failed, restarting. Reason: {e}')


if __name__ == '__main__':
    # Configure API
    config = configparser.ConfigParser()
    config.read('streamconfig.ini')
    token = config['primary']['API_TOKEN']
    alias = 'crystalmoon'  # Main account for trading
    id = gather_account_id(alias, token)

    # Configure global vars
    if config.has_option('primary', 'STREAM_URL'):
        STREAM_URL = config['primary']['STREAM_URL']

    if config.has_option('primary', 'MAX_QUEUE_LENGTH'):
        MAX_QUEUE_LENGTH = config['primary']['MAX_QUEUE_LENGTH']

    if config.has_option('primary', 'MAX_RECORDING_PROCESSES'):
        MAX_RECORDING_PROCESSES = config['primary']['MAX_RECORDING_PROCESSES']

    if config.has_option('primary', 'RESTART_INTERVAL'):
        RESTART_INTERVAL = config['primary']['RESTART_INTERVAL']

    instruments = gather_acct_instruments(id, token)

    # Script header
    # print('=' * 30)
    # print(f'Streaming {len(instruments)} instruments:')
    # pprint(instruments)
    # print('='*30)

    dispatch_processes(token, instruments)

