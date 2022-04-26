import sys
import time
import pymongo
import queue
from helpers import OANDA, load_config, seconds_to_human
from multiprocessing import Process, Manager


class DataGatherer:
    # Process number limits
    RECORDING_PROCESS_MIN = 1
    RECORDING_PROCESS_MAX = 64
    GATHERING_PROCESS_COUNT = 2
    PROCESSING_PROCESS_COUNT = 1

    # Periodic action intervals
    TICK_INTERVAL = 0.01
    DATA_REFRESH_INTERVAL = 60 * 10
    STATUS_INTERVAL = 5
    AUTOSCALE_INTERVAL = 0.25

    # Queue limits
    MAX_QUEUE_SIZE = 10
    QUEUE_TIMEOUT = 0.1

    def __init__(self, db_string: str, api_config: dict):
        self.db_string = db_string
        self.gathering_processes = []
        self.recording_processes = []
        self.processing_processes = []

        self.logger = create_logger()

        self.manager = Manager()
        self.latest_data = self.manager.list()
        self.unsaved_data = self.manager.Queue()
        self.unprocessed_data = self.manager.Queue()
        self.api_config = self.manager.dict()
        for key, value in api_config.items():
            self.api_config[key] = value

        self._previous_queue_sizes = {
            'unprocessed': 0,
            'unsaved': 0
        }
        self._queue_averages = {
            'unprocessed': 0,
            'unsaved': 0
        }
        self._autoscale_count = 0

    def _create_recording_process(self):
        recording_process = Process(
            target=self.record_data, args=(self.db_string, self.unsaved_data)
        )
        recording_process.daemon = True
        return recording_process

    def _create_processing_process(self):
        processing_process = Process(
            target=self.process_data,
            args=(self.latest_data, self.unprocessed_data, self.unsaved_data),
        )
        processing_process.daemon = True
        return processing_process

    def _create_gathering_process(self):
        gathering_process = Process(
            target=self.gather_data, args=(self.api_config, self.unprocessed_data)
        )
        gathering_process.daemon = True
        return gathering_process

    @staticmethod
    def process_datapoint(datapoint):
        processed_datapoint = {
            "time": datapoint["time"],
            "bid": datapoint["closeoutBid"],
            "ask": datapoint["closeoutAsk"],
            "status": datapoint["status"],
            "tradeable": datapoint["tradeable"],
            "instrument": datapoint["instrument"],
        }

        return {"dest": datapoint["instrument"], "data": processed_datapoint}

    @staticmethod
    def process_data(latest_data, unprocessed_queue, unsaved_queue):
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
    def gather_data(api_config, unprocessed_queue):
        logger = create_logger()
        token = api_config["token"]
        id = api_config["id"]
        instruments = api_config["instruments"]
        url = api_config["url"]

        data = OANDA.stream_prices(token, id, instruments, url)
        for datapoint in data:
            unprocessed_queue.put(datapoint)

        logger.critical('Data stream closed - terminating process.')

    @staticmethod
    def record_data(db_string, unsaved_queue):
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

    def refresh_data_stream(self):
        self.logger.warning('Creating new gathering process and killing old one...')
        gathering_process = self._create_gathering_process()
        gathering_process.start()
        self.gathering_processes.append(gathering_process)
        old_process = self.gathering_processes.pop(0)
        old_process.terminate()
        old_process.join()

    def _autoscale_gathering_processes(self):
        dead = [process for process in self.gathering_processes if not process.is_alive()]

        for process in dead:
            self.gathering_processes.remove(process)

        while len(self.gathering_processes) < DataGatherer.GATHERING_PROCESS_COUNT:
            self.logger.warning('Missing gathering process, creating new process.')
            new_proc = self._create_gathering_process()
            new_proc.start()
            self.gathering_processes.append(new_proc)

    def _downscale(self):
        self.logger.info("Terminating recording process to downscale.")
        downscale_process = self.recording_processes.pop(0)
        downscale_process.terminate()
        downscale_process.join()

    def _upscale(self):
        self.logger.info("Creating new recording process to handle load.")
        recording_process = self._create_recording_process()
        self.recording_processes.append(recording_process)
        self.recording_processes[-1].start()

    def _autoscale_recording_processes(self):
        unsaved_queue_size = self.unsaved_data.qsize()
        unprocessed_queue_size = self.unprocessed_data.qsize()
        queue_growth = (unsaved_queue_size - self._previous_queue_sizes['unsaved']) > 0

        exceeded_max_queue = unsaved_queue_size > DataGatherer.MAX_QUEUE_SIZE

        if (
                exceeded_max_queue
                and len(self.recording_processes) < DataGatherer.RECORDING_PROCESS_MAX
        ):
            self.logger.debug("Max queue size exceeded.")
            self._upscale()

        if (
                not exceeded_max_queue
                and not queue_growth
                and len(self.recording_processes) > DataGatherer.RECORDING_PROCESS_MIN
        ):
            self._downscale()

        self._previous_queue_sizes['unsaved'] = unsaved_queue_size
        self._previous_queue_sizes['unprocessed'] = unprocessed_queue_size

    def autoscale(self):
        self._autoscale_recording_processes()
        self._autoscale_gathering_processes()
        self._telemetry()

    def _telemetry(self):
        unsaved_queue_size = self.unsaved_data.qsize()
        unprocessed_queue_size = self.unprocessed_data.qsize()

        self._queue_averages['unsaved'] = self._calculate_moving_average(self._queue_averages['unsaved'], unsaved_queue_size, self._autoscale_count)
        self._queue_averages['unprocessed'] = self._calculate_moving_average(self._queue_averages['unprocessed'], unprocessed_queue_size, self._autoscale_count)
        self._autoscale_count += 1

    def _calculate_moving_average(self, previous, new, count):
        return previous * (count/(count+1)) + new * (1/(count+1))

    def run(self):
        for _ in range(DataGatherer.GATHERING_PROCESS_COUNT):
            gathering_process = self._create_gathering_process()
            self.gathering_processes.append(gathering_process)

        for _ in range(DataGatherer.PROCESSING_PROCESS_COUNT):
            processing_process = self._create_processing_process()
            self.processing_processes.append(processing_process)

        for _ in range(DataGatherer.RECORDING_PROCESS_MIN):
            recording_process = self._create_recording_process()
            self.recording_processes.append(recording_process)

        for process in self.recording_processes:
            process.start()

        for process in self.processing_processes:
            process.start()

        for process in self.gathering_processes:
            process.start()

        tick_count = 1
        tick_time = 0
        while True:
            tick_start = time.time()
            uptime = tick_count * DataGatherer.TICK_INTERVAL

            if uptime % DataGatherer.AUTOSCALE_INTERVAL == 0:
                self.autoscale()

            if uptime % DataGatherer.STATUS_INTERVAL == 0:
                proc_count_message = f'# Subprocesses: [Gathering: {len(self.gathering_processes)} | ' \
                                     f'Processing: {len(self.processing_processes)} | ' \
                                     f'Recording: {len(self.recording_processes)}]'
                queue_size_message = f"Queue Sizes (current | avg): " \
                                     f"[Unprocessed: ({self.unprocessed_data.qsize()} | {self._queue_averages['unprocessed']:.2f}) | " \
                                     f"Unsaved: ({self.unsaved_data.qsize()} | {self._queue_averages['unsaved']:.2f})]"
                timing_message = f"Timing: [Uptime: {seconds_to_human(uptime)} | Previous Tick Time: {tick_time:0.3e}]"

                self.logger.warning(proc_count_message)
                self.logger.warning(queue_size_message)
                self.logger.warning(timing_message)

            if uptime % DataGatherer.DATA_REFRESH_INTERVAL == 0:
                self.refresh_data_stream()

            tick_time = time.time() - tick_start
            sleep_time = DataGatherer.TICK_INTERVAL - tick_time
            sleep_time = 0 if sleep_time < 0 else sleep_time
            time.sleep(sleep_time)
            tick_count += 1

    def stop(self):
        for process in self.gathering_processes:
            process.terminate()
            process.join()

        for process in self.processing_processes:
            process.terminate()
            process.join()

        for process in self.gathering_processes:
            process.terminate()
            process.join()


def create_logger():
    import multiprocessing, logging

    logger = multiprocessing.get_logger()
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "[%(asctime)s| %(levelname)s| %(processName)s] %(message)s"
    )
    handler = logging.FileHandler("logs/latest.log")
    handler.setFormatter(formatter)

    if len(logger.handlers) < 1:
        logger.addHandler(handler)

    out_handler = logging.StreamHandler(sys.stdout)
    out_handler.setFormatter(formatter)
    out_handler.setLevel(logging.WARNING)

    if len(logger.handlers) < 2:
        logger.addHandler(out_handler)

    return logger


def main():
    logger = create_logger()
    logger.info("Starting data collection.")

    cfg = load_config()
    token = cfg["token"]
    alias = cfg["alias"]
    db_string = cfg["db_string"]

    api = OANDA.API(token, live=False)
    account = api.get_account(alias)
    if not account:
        print('Error connecting to ')
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
        raise


if __name__ == "__main__":
    main()
