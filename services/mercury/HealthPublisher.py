import zmq
import threading
import time

import logging

class HealthPublisher:
    def __init__(self, bind_to: str, health: dict):
        self.bind_to = bind_to
        self.health = health

        self.ctx = zmq.Context()
        self.s = self.ctx.socket(zmq.PUB)
        self.s.bind(self.bind_to)

        self.prev_message_id = -1

        self.publish_thread = threading.Thread(target=self.run)
        self.publish_thread.daemon = True
        self.logger = logging.getLogger(f'HealthPublisher')

    def publish(self) -> None:
        # print('Publishing health data...')
        self.s.send_json(self.health)

    def close(self) -> None:
        self.s.close()
        self.ctx.term()

    def run(self) -> None:
        self.logger.info(f'Publishing health data at {self.bind_to}...')

        while True:
            if int(self.health['timestamp']) != int(self.prev_message_id):
                self.publish()
                self.prev_message_id = self.health['timestamp']

            # else:
            #     print('Skipping publish, no change in health data...')
            # self.publish()
            time.sleep(0.1)

    def start(self):
        self.publish_thread.start()
        


if __name__ == "__main__":
    health = {
        'cpu': 0.5,
        'uptime': 1000,
        'connected': True,
        'id': 0
    }

    address = "tcp://*:5500"
    publisher = HealthPublisher(address, health)
    publisher.start()
    publisher.publish_thread.join()
    