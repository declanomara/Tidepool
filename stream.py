import sys
import configparser
import requests
import json
import time
import pymongo
import logging
import multiprocessing
import queue

from multiprocessing import Manager
from moonwrapper import gather_account_id, gather_acct_instruments, MoonQueue


STREAM_URL = 'https://stream-fxpractice.oanda.com/'
MAX_QUEUE_LENGTH = 30
MAX_RECORDING_PROCESSES = 10
RESTART_INTERVAL = 5*60
PROC_MANAGEMENT_INTERVAL = 1


def dispatch_processes(token, instruments):
    while True:
        m = Manager()
        q = MoonQueue()
        tq = m.list()
        recording_processes = list()
        recording_processes.append(multiprocessing.Process(target=record_data, args=(q,)))

        gathering_process = multiprocessing.Process(target=stream_prices, args=(token, instruments, q, tq))
        monitoring_process = multiprocessing.Process(target=monitor_queue, args=(q, recording_processes))
        stats_process = multiprocessing.Process(target=stat_keeper, args=(tq,))

        for p in [*recording_processes, gathering_process, monitoring_process, stats_process]:
            p.start()

        time.sleep(RESTART_INTERVAL) # Restart script every 5 hours

        logging.info('Restarting script...')

        for p in [*recording_processes, gathering_process, monitoring_process, stats_process]:
            p.terminate()

        del q
        del recording_processes
        del gathering_process
        del monitoring_process


def stat_keeper(tq):
    logging.info('Monitoring network performance...')

    max_speed = 0
    min_speed = 99999999
    while True:
        time.sleep(10)
        min_time = time.time()-10
        for i, t in enumerate(tq):
            if t < min_time:
                tq.pop(i)

        avg = len(tq)/10
        max_speed = avg if avg > max_speed else max_speed
        min_speed = avg if avg < min_speed else min_speed

        logging.info(f'Avg data speed: {avg}pt/s ({(avg*300)/1000:0.2f}KB/s)'
                     f' Max data speed: {max_speed}pt/s ({(max_speed*300)/1000:0.2f}KB/s)'
                     f' Min data speed: {min_speed}pt/s ({(min_speed*300)/1000:0.2f}KB/s)')


def monitor_queue(q: MoonQueue, recording_processes):
    while True:
        # Monitor queue size and adjust recording pool accordingly
        s = q.qsize()
        if s > MAX_QUEUE_LENGTH:
            logging.warning(f'Max queue length exceeded, current length: {s}')

            if len(recording_processes) < MAX_RECORDING_PROCESSES:
                # Spawn new process to handle excess load
                p = multiprocessing.Process(target=record_data, args=(q,))
                p.start()
                recording_processes.append(p)
                logging.warning(f'Spawned new data recording process. Process count: {len(recording_processes)}')

            elif len(recording_processes) == MAX_RECORDING_PROCESSES:
                # Replace oldest process in case recording is broken for some reason
                p = multiprocessing.Process(target=record_data, args=(q,))
                p.start()
                recording_processes.append(p)

                recording_processes.pop(0).terminate()
                logging.warning(f'Already at max processes, recycling a process. Process count: {len(recording_processes)}')

            else:
                recording_processes.pop(0).terminate()
                logging.warning(f'Max processes exceeded, terminating a process. Process count: {len(recording_processes)}')

        if s < MAX_QUEUE_LENGTH and len(recording_processes) > 1:
            recording_processes.pop(0).terminate()
            logging.warning(f'Killed data recording process. Process count: {len(recording_processes)}')

        time.sleep(PROC_MANAGEMENT_INTERVAL)


def record_data(q: MoonQueue):
    # Configure DB
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    logging.info('Connection to database established')
    crystalmoondb = client['crystalmoon']
    collection = crystalmoondb['raw']

    while True:
        logging.debug('Pulling data from queue...')
        try:
            data_point = q.get(timeout=1)
            logging.debug('Inserting data into database...')
            collection.insert_one(data_point)
            logging.debug('Data recorded.')

        except queue.Empty:
            logging.debug('Queue appears to be empty')


def stream_prices(token,  instruments, q: MoonQueue, tq: list):
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
                        tq.append(time.time())

                    if data_points % 100 == 0:
                        logging.debug(f'Gathered {data_points} new data points...')

        except Exception as e:
            logging.error(f'API connection failed, restarting. Reason: {e}')


if __name__ == '__main__':
    # Configure logging
    log = f'logs/{time.strftime("%Y%m%d-%H%M%S")}-streaming.log'
    logging.basicConfig(filename=log, format='%(levelname)s:%(message)s', level=logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s]: %(message)s', "%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    logging.getLogger().addHandler(handler)

    # Configure API
    logging.debug('Configuring API...')
    config = configparser.ConfigParser()
    config.read('streamconfig.ini')
    token = config['primary']['API_TOKEN']
    alias = 'crystalmoon'  # Main account for trading
    id = gather_account_id(alias, token)

    # Configure global vars
    if config.has_option('primary', 'STREAM_URL'):
        STREAM_URL = config['primary']['STREAM_URL']

    if config.has_option('primary', 'MAX_QUEUE_LENGTH'):
        MAX_QUEUE_LENGTH = int(config['primary']['MAX_QUEUE_LENGTH'])

    if config.has_option('primary', 'MAX_RECORDING_PROCESSES'):
        MAX_RECORDING_PROCESSES = int(config['primary']['MAX_RECORDING_PROCESSES'])

    if config.has_option('primary', 'RESTART_INTERVAL'):
        RESTART_INTERVAL = int(config['primary']['RESTART_INTERVAL'])

    instruments = gather_acct_instruments(id, token)

    logging.info('Dispatching processes to begin data collection...')
    try:
        dispatch_processes(token, instruments)

    except KeyboardInterrupt:
        logging.ERROR('KeyboardInterrupt detecting, quitting program.')
        [p.terminate() for p in multiprocessing.active_children()]

