import time
import json
import zmq
import logging
import argparse

from pathlib import Path
from multiprocessing import Process, Queue, Manager
from tidepool.compatible_queue import CompatibleQueue
from tidepool.OANDA import OANDA, Account
from tidepool.utils import create_logger, get_multiprocessing_logger
from tidepool.scaling import ScalableGroup, LoadBalancer
from HealthPublisher import HealthPublisher


class DataCollector:
    def __init__(self, account: Account, cfg: dict, logging_queue: Queue = None) -> None:
        self.logger = logging.getLogger(f"DataCollector")
        self.logging_queue = logging_queue

        self.account = account
        self.instruments = cfg["instruments"]

        self.manager = Manager()

        self.push_telemetry = self.manager.dict({"data_count": 0})
        self.validated_queue = CompatibleQueue()
        self.data_pusher = LoadBalancer(
            self.push_data,
            self.validated_queue,
            args=(cfg["dataPusher"]["port"], self.push_telemetry),
            kwargs={"logging_queue": self.logging_queue},
            min_processes=cfg["dataPusher"]["minProcesses"],
            max_processes=cfg["dataPusher"]["maxProcesses"],
        )

        self.validation_telemetry = self.manager.dict({"data_count": 0})
        self.unvalidated_queue = CompatibleQueue()
        self.data_validator = LoadBalancer(
            self.validate_data,
            self.unvalidated_queue,
            args=(self.validated_queue, self.validation_telemetry),
            kwargs={"logging_queue": self.logging_queue},
            min_processes=cfg["dataValidator"]["minProcesses"],
            max_processes=cfg["dataValidator"]["maxProcesses"],
        )

        self.collection_telemetry = self.manager.dict({"data_count": 0})
        self.data_collector = ScalableGroup(
            self.collect_data,
            args=(
                self.unvalidated_queue,
                self.account,
                self.instruments,
                self.collection_telemetry,
            ),
            kwargs={"logging_queue": self.logging_queue},
            min_processes=2,
        )

        self.start_time = -1
        self.health = {
            "data_collector": {"data_count": 0, "process_count": 0},
            "data_validator": {"data_count": 0, "process_count": 0},
            "server": {"uptime": 0},
            "timestamp": 0,
        }
        self.address = f'{cfg["health"]["host"]}:{cfg["health"]["port"]}'

        self.health_publisher = HealthPublisher(self.address, self.health)

    @staticmethod
    def collect_data(queue: Queue, account: Account, instruments: list[str], telemetry: dict = None, logging_queue: Queue = None) -> None:
        logger = get_multiprocessing_logger(f"collect_data", logging_queue)
        if telemetry is None:
            telemetry = {}

        try:
            for price in account.stream_prices(instruments):
                queue.put(price.decode("utf-8"))
                telemetry["data_count"] += 1

        except KeyboardInterrupt:
            pass

    @staticmethod
    def validate_data(unvalidated_queue: Queue, validated_queue: Queue, telemetry: dict = None, logging_queue: Queue = None) -> None:
        logger = get_multiprocessing_logger(f"validate_data", logging_queue)

        def validate_price(datapoint):
            required_fields = [
                "type",
                "time",
                "bids",
                "asks",
                "closeoutBid",
                "closeoutAsk",
                "status",
                "tradeable",
                "instrument",
            ]

            for field in required_fields:
                if field not in datapoint:
                    logger.warning(f"Price data point is missing field: '{field}'")
                    return False

            return True

        def validate_heartbeat(datapoint):
            required_fields = ["type", "time"]

            for field in required_fields:
                if field not in datapoint:
                    logger.warning(f"Heartbeat data point is missing field: '{field}'")
                    return False

            return True

        if telemetry is None:
            telemetry = {"data_count": 0}

        while True:
            item = unvalidated_queue.get()
            datapoint = json.loads(item)

            if "type" not in datapoint:
                logger.warning("Data point is missing field: 'type'")

            if datapoint["type"] == "PRICE":
                if not validate_price(datapoint):
                    continue

            elif datapoint["type"] == "HEARTBEAT":
                if not validate_heartbeat(datapoint):
                    continue

            validated_queue.put(item)
            telemetry["data_count"] += 1

    @staticmethod
    def push_data(queue: Queue, port: int, telemetry: dict = None, logging_queue: Queue = None) -> None:
        logger = get_multiprocessing_logger(f"push_data", logging_queue)

        if telemetry is None:
            telemetry = {
                "data_count": 0,
            }

        try:
            context = zmq.Context()
            socket = context.socket(zmq.PUSH)
            socket.bind(f"tcp://*:{port}")
            logger.info(f"Output data bound to port {port}")

        except zmq.error.ZMQError as e:
            logger.error(f"Failed to bind to port {port}: {e}")
            return

        while True:
            try:
                item = queue.get()
                socket.send_string(item)

                telemetry["data_count"] += 1

            except KeyboardInterrupt:
                break


    def autoscale(self) -> None:
        self.data_pusher.autoscale()
        self.data_validator.autoscale()
        self.data_collector.autoscale()

    def update_health(self) -> None:
        """
        Defines the structure of the health dictionary to be published to the health monitor.
        """

        self.health["data_collector"] = {
            "num_processes": self.data_collector.process_count(),
            "action_count": self.collection_telemetry["data_count"],
        }

        self.health["data_validator"] = {
            "num_processes": self.data_validator.process_count(),
            "action_count": self.validation_telemetry["data_count"],
            "queue_size": self.data_validator.queue_size(),
        }

        self.health["data_pusher"] = {
            "num_processes": self.data_pusher.process_count(),
            "action_count": self.push_telemetry["data_count"],
            "queue_size": self.data_pusher.queue_size(),
        }

        self.health["server"] = {"uptime": time.time() - self.start_time}

        self.health["timestamp"] = time.time()

    def tick(self, tick_number: int) -> None:
        """
        The tick function is responsible for all logic behind the DataCollector. It is the main loop of the DataCollector and is called every second.
        It comprises of a number of periodic tasks, such as autoscaling and logging.
        """
        if tick_number % 2 == 0:
            self.logger.debug("Autoscaling...")
            self.autoscale()
            self.logger.debug(f"Autoscaling complete.")

        if tick_number % 5 == 0:
            self.logger.info("Current Status:")
            self.logger.info(f"    [Data Collector] Process Count: {self.data_collector.process_count()}")
            self.logger.info(
                f"    [Data Validator] Process Count: {self.data_validator.process_count()} Queue Size: {self.unvalidated_queue.qsize()}"
            )
            self.logger.info(
                f"    [Data Pusher] Process Count: {self.data_pusher.process_count()} Queue Size: {self.validated_queue.qsize()}"
            )
            self.logger.info(
                f'    [Aggregate] Data Collected: {self.collection_telemetry["data_count"]} Data Validated: {self.validation_telemetry["data_count"]} Data Pushed: {self.push_telemetry["data_count"]}'
            )

    def start(self) -> None:
        """
        The start function is responsible for starting the DataCollector. It should be called only once.
        """
        self.start_time = time.time()

        self.health_publisher.start()
        self.logger.info("Started health publisher process.")

        self.data_pusher.start()
        self.logger.info("Started data pusher process.")

        self.data_validator.start()
        self.logger.info("Started data validator processes.")

        self.data_collector.start()
        self.logger.info("Started data collector processes.")

    def run(self) -> None:
        """
        This is the entrypoint of the DataCollector.
        The run function starts the DataCollector. The only two functions of the run function beyond this are to call the tick function,
        and to update the health of the DataCollector between ticks. All logic should be handled in the tick function.
        """

        self.start()
        tick_number = 1
        while True:
            tick_start = time.time()
            self.tick(tick_number)
            tick_number += 1
            tick_end = time.time()
            tick_time = tick_end - tick_start

            wake_time = time.time() + (1 - tick_time)
            self.logger.debug(f"Tick took {tick_time} seconds")

            while time.time() < wake_time:
                self.logger.debug("Updating health...")
                self.update_health()
                time.sleep(0.1)


if __name__ == "__main__":
    # Create logger
    logging_queue = CompatibleQueue()
    log_file = Path(f"/usr/local/tidepool/logs/mercury.log")
    create_logger(log_file, logging_queue=logging_queue)
    logger = logging.getLogger("__main__")

    parser = argparse.ArgumentParser(
                    prog = 'Tidepool Data Collector',
                    description = 'Collects data from OANDA and pushes it to the Tidepool data deduplicator.',
                    epilog = 'Created by: Declan O.')

    # Add arguments
    parser.add_argument('-i', type=int, help='The index of the config file to use.')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging.')
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug logging.')

    # Parse arguments
    args = parser.parse_args()

    # Load config file
    if not args.i:
        logger.error("No config file index provided. Use -i to specify the index of the config file to use.")
        exit(1)

    installed_cfg_path = Path(f"/usr/local/tidepool/configs/mercury{args.i}.json")
    relative_cfg_path = Path(f"configs/mercury{args.i}.json")
    
    if installed_cfg_path.exists():
        cfg_path = installed_cfg_path
    elif relative_cfg_path.exists():
        cfg_path = relative_cfg_path
    else:
        logger.error(f"Could not find config file at {installed_cfg_path} or {relative_cfg_path}.")
        exit(1)

    cfg = json.loads(open(cfg_path).read())

    token = cfg["token"]
    api = OANDA(token, live=cfg["live"])
    account = api.get_account(cfg["alias"])

    if not cfg["useInstruments"]:
        cfg["instruments"] = [instrument.name for instrument in account.get_instruments()]

    logger.info(f"Successfully loaded config file: {cfg_path}")
    logger.info(f"    [Account] {cfg['alias']}")
    logger.info(f"    [Live] {cfg['live']}")
    logger.info(f"    [Data Port] {cfg['dataPusher']['port']}")
    logger.info(f"    [Health Port] {cfg['health']['port']}")

    if len(cfg["instruments"]) > 3:
        logger.info(f"    [Instruments] {cfg['instruments'][0]}, {cfg['instruments'][1]}, {cfg['instruments'][2]}...+{len(cfg['instruments']) - 3}")

    else:
        logger.info(f"    [Instruments] {cfg['instruments']}")

    collector = DataCollector(account, cfg, logging_queue)

    logger.info(f"Starting DataCollector in 3 seconds...")
    time.sleep(3)

    try:
        collector.run()

    except KeyboardInterrupt:
        collector.data_validator.stop()
        collector.data_collector.stop()
        logger.info("Stopped.")
