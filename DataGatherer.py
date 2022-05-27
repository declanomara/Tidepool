import time
import pymongo
import queue

from dateutil import parser
from queue import Queue
from helpers import OANDA
from helpers.misc import (
    load_config,
    seconds_to_human,
    ignore_keyboard_interrupt,
    create_logger,
)
from helpers.balancing import LoadBalancer, AutoscalingGroup
from multiprocessing import Manager


class DataGatherer:
    # Process number limits
    GATHERING_PROCESS_COUNT_MIN = 2

    RECORDING_PROCESS_COUNT_MIN = 1
    RECORDING_PROCESS_COUNT_MAX = 64

    PROCESSING_PROCESS_COUNT_MIN = 1
    PROCESSING_PROCESS_COUNT_MAX = 10

    # Periodic action intervals
    TICK_INTERVAL = 0.01
    DATA_REFRESH_INTERVAL = 60 * 10
    STATUS_INTERVAL = 5
    AUTOSCALE_INTERVAL = 0.25

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
        self.api_config = self.manager.dict()
        for key, value in api_config.items():
            self.api_config[key] = value

        self.gatherers = AutoscalingGroup(
            self.gather_data, (self.api_config, self.unprocessed_data), 2
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

        return {"dest": datapoint["instrument"], "data": processed_datapoint}

    @staticmethod
    @ignore_keyboard_interrupt
    def process_data(unprocessed_queue: Queue, latest_data: list, unsaved_queue: Queue) -> None:
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

                # Save every datapoint
                unsaved_queue.put({"dest": "raw", "data": datapoint})

                # Save relevant price data in correct database
                if datapoint["type"] == "PRICE":
                    unsaved_queue.put(DataGatherer.process_datapoint(datapoint))

            except queue.Empty:
                logger.debug("Unprocessed queue is empty.")

    @staticmethod
    @ignore_keyboard_interrupt
    def gather_data(api_config: dict, unprocessed_queue: Queue) -> None:
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

        logger.critical("Data stream closed - terminating process.")

    @staticmethod
    @ignore_keyboard_interrupt
    def record_data(unsaved_queue: Queue, db_string: str) -> None:
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
                if datapoint["dest"] == "raw":
                    raw.insert_one(datapoint["data"])

                else:
                    logger.info("Inserting data point into db...")
                    start = time.time_ns()
                    tidepooldb[datapoint["dest"]].insert_one(datapoint["data"])
                    elapsed = (time.time_ns() - start) / 1000
                    logger.info(f"Successfully inserted data point in {elapsed}μs")

            except queue.Empty:
                logger.debug("Unsaved queue is empty.")

    def autoscale(self) -> None:
        """
        :rtype: None
        """
        self.gatherers.autoscale()
        self.processors.autoscale()
        self.recorders.autoscale()

    def run(self) -> None:
        """
        :rtype: None
        """
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
                timing_message = f"Timing: [Uptime: {seconds_to_human(uptime)} | Previous Tick Time: {tick_time:0.3e}]"

                self.logger.warning(proc_count_message)
                self.logger.warning(queue_size_message)
                self.logger.warning(timing_message)

            if uptime % DataGatherer.DATA_REFRESH_INTERVAL == 0:
                self.gatherers.refresh_procs()

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
