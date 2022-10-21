import time
import pymongo
import queue

from dateutil import parser
from queue import Queue
from helpers import OANDA
from helpers.misc import (
    load_config,
    seconds_to_human,
    seconds_to_us,
    ignore_keyboard_interrupt,
    create_logger,
)
from helpers.ipc import expose_telemetry, telemetry_format
from helpers.balancing import LoadBalancer, AutoscalingGroup
from multiprocessing import Manager


def process_datapoint(datapoint: dict) -> dict:
    """
    :param datapoint: Raw OANDA datapoint in dictionary format
    :return: Returns a datapoint formatted for database storage
    :rtype: dict
    """
    processed_datapoint = {
        "time": parser.parse(datapoint["time"]),
        "bid": datapoint["closeoutBid"],
        "ask": datapoint["closeoutAsk"],
        "status": datapoint["status"],
        "tradeable": datapoint["tradeable"],
        "instrument": datapoint["instrument"],
    }

    db_packet = {"dest": datapoint["instrument"], "data": processed_datapoint}
    return db_packet


class DataGatherer:
    # Process number limits
    GATHERING_PROCESS_COUNT_MIN = 2

    RECORDING_PROCESS_COUNT_MIN = 1
    RECORDING_PROCESS_COUNT_MAX = 64

    PROCESSING_PROCESS_COUNT_MIN = 1
    PROCESSING_PROCESS_COUNT_MAX = 10

    # Periodic action intervals (perform action every n seconds)
    TICK_INTERVAL = 0.01
    DATA_REFRESH_INTERVAL = 60 * 10
    STATUS_INTERVAL = 5
    AUTOSCALE_INTERVAL = 0.25
    UPDATE_TELEMETRY_INTERVAL = 5

    # Queue limits
    MAX_QUEUE_SIZE = 10
    QUEUE_TIMEOUT = 0.1

    def __init__(self, db_string: str, api_config: dict) -> None:
        """
        :param db_string: MongoDB database connection string
        :param api_config: API config dictionary
        :rtype: None
        """
        self.db_string = db_string
        self.logger = create_logger()

        self.manager = Manager()
        self.latest_data = self.manager.list()
        self.unsaved_data = self.manager.Queue()
        self.unprocessed_data = self.manager.Queue()

        self.public_telemetry = self.manager.dict(telemetry_format)

        self.api_config = self.manager.dict()
        for key, value in api_config.items():
            self.api_config[key] = value

        self.ipc = AutoscalingGroup(expose_telemetry, (self.public_telemetry,), 1)

        self.gatherers = AutoscalingGroup(
            self.gather_data,
            (self.api_config, self.unprocessed_data),
            DataGatherer.GATHERING_PROCESS_COUNT_MIN,
        )

        self.processors = LoadBalancer(
            self.process_data,
            (self.latest_data, self.unsaved_data),
            DataGatherer.PROCESSING_PROCESS_COUNT_MIN,
            DataGatherer.PROCESSING_PROCESS_COUNT_MAX,
            self.unprocessed_data,
            DataGatherer.MAX_QUEUE_SIZE,
        )

        self.recorders = LoadBalancer(
            self.record_data,
            (self.db_string,),
            DataGatherer.RECORDING_PROCESS_COUNT_MIN,
            DataGatherer.RECORDING_PROCESS_COUNT_MAX,
            self.unsaved_data,
            DataGatherer.MAX_QUEUE_SIZE,
        )

    @staticmethod
    @ignore_keyboard_interrupt
    def gather_data(
        api_config: dict, unprocessed_queue: Queue, telemetry: dict = None
    ) -> None:
        """
        :param api_config: API config dictionary
        :param unprocessed_queue: Queue for unprocessed data points
        :rtype: None
        """
        logger = create_logger()
        token = api_config["token"]
        id = api_config["id"]
        instruments = api_config["instruments"]
        url = api_config["url"]

        data = OANDA.stream_prices(token, id, instruments, url)
        for datapoint in data:
            unprocessed_queue.put(datapoint)

            if telemetry is not None:
                # do telemetry...
                telemetry["action_count"] += 1

        logger.critical("Data stream closed - terminating process.")

    @staticmethod
    @ignore_keyboard_interrupt
    def process_data(
        unprocessed_queue: Queue,
        latest_data: list,
        unsaved_queue: Queue,
        telemetry: dict = None,
    ) -> None:
        """
        :param unprocessed_queue: Queue containing unprocessed data points
        :param latest_data: List of recently processed data points for removing duplicates
        :param unsaved_queue: Queue of unsaved data points (used by saving processes)
        :rtype: None
        """
        logger = create_logger()
        while True:
            try:
                unprocessed_datapoint = unprocessed_queue.get(
                    timeout=DataGatherer.QUEUE_TIMEOUT
                )
                datapoint = unprocessed_datapoint
                if datapoint in latest_data:
                    continue

                latest_data.append(datapoint)
                if len(latest_data) > 100:
                    latest_data.pop(0)

                logger.info(f"Processing datapoint: {datapoint}")
                # Save every datapoint
                to_queue = {"dest": "raw", "data": datapoint}
                unsaved_queue.put(to_queue)

                # Save relevant price data in correct database
                if datapoint["type"] == "PRICE":
                    to_queue = process_datapoint(datapoint)
                    unsaved_queue.put(to_queue)

                if telemetry is not None:
                    telemetry["action_count"] += 1

            except queue.Empty:
                logger.debug("Unprocessed queue is empty.")

    @staticmethod
    @ignore_keyboard_interrupt
    def record_data(
        unsaved_queue: Queue, db_string: str, telemetry: dict = None
    ) -> None:
        """
        :param unsaved_queue: Queue containing unsaved datapoints
        :param db_string: MongoDB connection string
        :rtype: None
        """
        logger = create_logger()
        client = pymongo.MongoClient(db_string)
        tidepooldb = client["tidepool"]
        raw = tidepooldb["raw"]

        while True:
            try:
                datapoint = unsaved_queue.get(timeout=DataGatherer.QUEUE_TIMEOUT)

                if datapoint is None:
                    logger.error("'NoneType' datapoint found in unsaved queue.")
                    continue

                if datapoint["dest"] == "raw":
                    raw.insert_one(datapoint["data"])

                else:
                    logger.info("Inserting data point into db...")
                    start = time.time_ns()
                    tidepooldb[datapoint["dest"]].insert_one(datapoint["data"])
                    elapsed = (time.time_ns() - start) / 1000
                    logger.info(f"Successfully inserted data point in {elapsed}μs")

                if telemetry is not None:
                    telemetry["action_count"] += 1

            except queue.Empty:
                logger.debug("Unsaved queue is empty.")

    def calculate_data_rates(self) -> tuple:
        total_data_gathered, total_data_processed, total_data_recorded = self.get_data_totals()

        data_gather_rate: float = (
                                   total_data_gathered - self.public_telemetry["data"]["gatherers"]["total"]
                           ) / DataGatherer.UPDATE_TELEMETRY_INTERVAL
        data_process_rate: float = (
                                    total_data_processed - self.public_telemetry["data"]["processors"]["total"]
                            ) / DataGatherer.UPDATE_TELEMETRY_INTERVAL
        data_record_rate: float = (
                                   total_data_recorded - self.public_telemetry["data"]["recorders"]["total"]
                           ) / DataGatherer.UPDATE_TELEMETRY_INTERVAL

        return data_gather_rate, data_process_rate, data_record_rate

    def get_data_totals(self) -> tuple:
        total_data_gathered = self.gatherers.telemetry["action_count"]
        total_data_processed = self.processors.telemetry["action_count"]
        total_data_recorded = self.recorders.telemetry["action_count"]

        return total_data_gathered, total_data_processed, total_data_recorded

    def update_telemetry(self, uptime: float):
        data_gather_rate, data_process_rate, data_record_rate = self.calculate_data_rates()
        total_data_gathered, total_data_processed, total_data_recorded = self.get_data_totals()

        public_telemetry = {
            'data': {
                'gatherers': {
                    'total': total_data_gathered,
                    'rate': data_gather_rate
                },
                'processors': {
                    'total': total_data_processed,
                    'rate': data_process_rate
                },
                'recorders': {
                    'total': total_data_recorded,
                    'rate': data_record_rate
                }
            },
            'server': {
                'proc_counts': {
                    'gatherers': self.gatherers.proc_count(),
                    'processors': self.processors.proc_count(),
                    'recorders': self.recorders.proc_count()
                },
                'queues': {
                    'unprocessed': {
                        'size': self.unprocessed_data.qsize(),
                        'average': self.processors.queue_average
                    },
                    'unsaved': {
                        'size': self.unsaved_data.qsize(),
                        'average': self.recorders.queue_average
                    }
                },
                'uptime': uptime

            }
        }

        self.public_telemetry.update(public_telemetry)

    def autoscale(self) -> None:
        """
        :rtype: None
        """
        self.ipc.autoscale()
        self.gatherers.autoscale()
        self.processors.autoscale()
        self.recorders.autoscale()

    def run(self) -> None:
        """
        :rtype: None
        """
        self.ipc.start()
        self.gatherers.start()
        self.processors.start()
        self.recorders.start()

        tick_count = 1
        tick_time = 0
        while True:
            tick_start = time.time()
            uptime = tick_count * DataGatherer.TICK_INTERVAL

            gathering_proc_count = self.gatherers.proc_count()
            processing_proc_count = self.processors.proc_count()
            recording_proc_count = self.recorders.proc_count()

            if uptime % DataGatherer.AUTOSCALE_INTERVAL == 0:
                self.autoscale()

            if uptime % DataGatherer.STATUS_INTERVAL == 0:
                proc_count_message = (
                    f"# Subprocesses: [Gathering: {gathering_proc_count} | "
                    f"Processing: {processing_proc_count} | "
                    f"Recording: {recording_proc_count}]"
                )
                queue_size_message = (
                    f"Queue Sizes (current | avg): "
                    f"[Unprocessed: ({self.unprocessed_data.qsize()} | {self.processors.queue_average:.2f}) | "
                    f"Unsaved: ({self.unsaved_data.qsize()} | {self.recorders.queue_average:.2f})]"
                )
                timing_message = f"Timing: [Uptime: {seconds_to_human(uptime)} | Previous Tick Time: {seconds_to_us(tick_time)}µs]"

                self.logger.warning(proc_count_message)
                self.logger.warning(queue_size_message)
                self.logger.warning(timing_message)

            if uptime % DataGatherer.DATA_REFRESH_INTERVAL == 0:
                self.gatherers.refresh_procs()

            if uptime % DataGatherer.UPDATE_TELEMETRY_INTERVAL == 0:
                self.update_telemetry(uptime)

            tick_time = time.time() - tick_start
            sleep_time = DataGatherer.TICK_INTERVAL - tick_time
            sleep_time = 0 if sleep_time < 0 else sleep_time
            time.sleep(sleep_time)
            tick_count += 1

    def stop(self) -> None:
        """
        :rtype: None
        """
        self.gatherers.stop()
        self.processors.stop()
        self.recorders.stop()


def main() -> None:
    """
    :rtype: None
    """
    logger = create_logger()
    logger.info("Starting data collection.")

    cfg = load_config()
    token = cfg["token"]
    alias = cfg["alias"]
    db_string = cfg["db_string"]
    live = cfg["live"]

    api = OANDA.API(token, live=live)
    account = api.get_account(alias)
    if not account:
        print("Error connecting to ")
    instruments = api.get_instruments(alias)

    api_config = {
        "url": api.stream_url,
        "token": token,
        "id": account["id"],
        "instruments": instruments,
    }

    dg = DataGatherer(db_string=db_string, api_config=api_config)
    try:
        dg.run()

    except KeyboardInterrupt:
        logger.critical("KeyboardInterrupt, stopping data collection.")
        dg.stop()
        quit(0)


if __name__ == "__main__":
    main()
