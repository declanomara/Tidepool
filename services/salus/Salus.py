import logging
import time
import zmq

from pathlib import Path
from threading import Thread
from typing import Callable, Generator
from tidepool.utils import create_logger


# Convert an integer to the corresponding health data port number
# Health ports are in the range 7100-7199
def health_port(id: int) -> int:
    return 7100 + id

class HealthMonitor:
    def __init__(self, connect_to: str):
        self.connect_to = connect_to

        self.ctx = zmq.Context()
        self.s = self.ctx.socket(zmq.SUB)    

        self.logger = logging.getLogger(f'HealthMonitor')   

    def connect(self) -> None:
        self.s.connect(self.connect_to)
        self.s.setsockopt(zmq.SUBSCRIBE, b'')

    def receive(self) -> dict:
        return self.s.recv_json()

    def close(self) -> None:
        self.s.close()
        self.ctx.term()

    def run(self) -> Generator[str, None, None]:
        self.logger.info(f'Connecting to health data at {self.connect_to}...')
        self.connect()

        while True:
            data = self.receive()
            yield data


class HealthDataCollection:
    def __init__(self, max_size: int = 1000):
        # health data contains the past 1000 health messages
        self.health_data = []
        self.max_size = max_size

    def update(self, data: dict) -> None:
        self.health_data.append(data)

        if len(self.health_data) > self.max_size:
            self.health_data.pop(0)
        
    def time_since_last(self) -> int:
        if len(self.health_data) == 0:
            return None
        return time.time() - self.health_data[-1]['timestamp']
    
    def latest(self) -> dict:
        return self.health_data[-1]

    def average(self, key: Callable, n: int = None, past: float = None) -> float:
        if n is None and past is None:
            raise ValueError('Either number of data points or time frame must be specified')
        
        if n is not None:
            if n > len(self.health_data):
                n = len(self.health_data)

            data = self.health_data[-n:]
        
        elif past is not None:
            data = [d for d in self.health_data if d['timestamp'] > time.time() - past]

        if len(data) == 0:
            return None

        return sum([key(d) for d in data]) / len(data)

    def velocity(self, key: Callable, n: int = None, past: float = None) -> float:
        # Returns the change in key per data point if n is specified, or per second if past is specified
        if n is None and past is None:
            raise ValueError('Either number of data points or time frame must be specified')

        if n is not None:
            if n > len(self.health_data):
                n = len(self.health_data)

            data = self.health_data[-n:]
            if len(data) == 0:
                return None

            return (key(data[-1]) - key(data[0])) / len(data)

        elif past is not None:
            data = [d for d in self.health_data if d['timestamp'] > time.time() - past]
            if len(data) == 0:
                return None

            return (key(data[-1]) - key(data[0])) / past


class DataCollectorMonitor:
    DEGRADED_WINDOW = 60 # seconds
    DEGRADED_THRESHOLD = 5 # actions per second

    def __init__(self, indices: list[int]):
        self.logger = logging.getLogger('__main__.Monitor')

        self.hostname = "tcp://127.0.0.1"
        self.ports = [health_port(i) for i in indices]
        self.health_collections = {port: HealthDataCollection() for port in self.ports}

        self.monitors = [Thread(target=self.monitor, args=(port,)) for port in self.ports]


    def monitor(self, port: int) -> None:
        address = f'{self.hostname}:{port}'
        monitor = HealthMonitor(address)

        for data in monitor.run():
            self.health_collections[port].update(data)

    def start(self) -> None:
        for monitor in self.monitors:
            monitor.daemon = True
            monitor.start()

    def status(self, index: int) -> tuple[int, str]:
        # Returns the status of the data collector with the given index
        # Status is a tuple of (status_code, status_message)
        # Status codes:
        #   0: OK
        #   1: Degraded
        #   2: Error
        #   3: Unknown
        # If the data collector cannot be reached, the status is unknown
        # If the data collector has not transmitted any data in the last 5 seconds, the status is error
        # If either queue is larger than 1000, the status is degraded
        # If the average number of actions per second is less than 5 over the past minute, the status is degraded
        # Otherwise, the status is OK

        port = health_port(index)
        if port not in self.health_collections or self.health_collections[port].time_since_last() is None:
            return 3, 'Unknown'

        if time_since_last := self.health_collections[port].time_since_last():
            if time_since_last > 5:
                return 2, 'Error'
        else:
            return 2, 'Error'

        data_pusher_queue_size = self.health_collections[port].latest()['data_pusher']['queue_size']
        if data_pusher_queue_size > 1000:
            return 1, 'Degraded: Data pusher queue size is too large'
        
        if data_pusher_queue_size > 10000:
            return 2, 'Error: Data pusher queue size is too large'
        
        data_validator_queue_size = self.health_collections[port].latest()['data_validator']['queue_size']
        if data_validator_queue_size > 1000:
            return 1, 'Degraded: Data validator queue size is too large'

        if data_validator_queue_size > 10000:
            return 2, 'Error: Data validator queue size is too large'

        if (aps := self.health_collections[port].velocity(lambda d: d['data_collector']['action_count'], past=self.DEGRADED_WINDOW)) < self.DEGRADED_THRESHOLD:
            return 1, f'Degraded: Average number of actions per second is too low ({aps:.2f} aps)'
        
        return 0, 'OK'
    
    def run(self) -> None:
        self.start()

        while True:
            for port in self.ports:
                avg = self.health_collections[port].velocity(lambda d: d['data_collector']['action_count'], past=10)
                if avg:
                    self.logger.info(f'Port {port}: {avg:.2f} actions/s')

                if time_since_last := self.health_collections[port].time_since_last():
                    if time_since_last > 5:
                        self.logger.error(f'Port {port} has not received any data in the last 5 seconds')
                else:
                    self.logger.error(f'Port {port} has not received any data')

            time.sleep(1)

    def actions_per_second(self, index: int, task: str, window: int = 10) -> float:
        # Returns the average number of actions per second in the given task in the last $window seconds
        # Tasks: data_collector, data_pusher, data_validator

        port = health_port(index)
        return self.health_collections[port].velocity(lambda d: d[task]['action_count'], past=window)


if __name__=='__main__':
    log_file = Path('/usr/local/tidepool/logs/salus.log')
    create_logger(log_file)
    logger = logging.getLogger('Main')

    indices = [1, 2]
    monitor = DataCollectorMonitor(indices)
    monitor.start()
    window = 30
    time.sleep(3)

    while True:
        for index in indices:
            status_code, status_message = monitor.status(index)
            collected_per_second = monitor.actions_per_second(index, 'data_collector', window=window)
            validated_per_second = monitor.actions_per_second(index, 'data_validator', window=window)
            pushed_per_second = monitor.actions_per_second(index, 'data_pusher', window=window)

            collected_per_second = collected_per_second if collected_per_second is not None else 0.0
            validated_per_second = validated_per_second if validated_per_second is not None else 0.0
            pushed_per_second = pushed_per_second if pushed_per_second is not None else 0.0

            # Log the status of the data collector with the given index as well as the average number of actions per second in the last 10 seconds in each category
            logger.info(f'Mercury@{index}: {status_code}, {status_message}')
            logger.info(f'    Data Collector: {collected_per_second:.2f} pts/s')
            logger.info(f'    Data Validator: {validated_per_second:.2f} pts/s')
            logger.info(f'    Data Pusher: {pushed_per_second:.2f} pts/s')
           
        time.sleep(5)
    