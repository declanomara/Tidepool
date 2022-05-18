import time
import pymongo
import queue
from dateutil import parser
from helpers import OANDA, load_config, seconds_to_human, ignore_keyboard_interrupt, moving_average, create_logger
from multiprocessing import Process, Manager


class AutoscalingGroup:
    def __init__(self, target, args, min_process_count):
        self.target = target
        self.args = args
        self.min_process_count = min_process_count

        self.processes = []

    def refresh_procs(self):
        for _ in range(self.proc_count()):
            self._downscale()
            self._upscale()

    def _create_proc(self):
        process = Process(target=self.target, args=self.args)
        process.daemon = True
        return process

    def stop(self):
        while self.proc_count() > 0:
            self._downscale()

    def start(self):
        for _ in range(self.min_process_count):
            process = self._create_proc()
            self.processes.append(process)

        for process in self.processes:
            process.start()

    def proc_count(self):
        return len(self.processes)

    def _upscale(self):
        new_proc = self._create_proc()
        new_proc.start()
        self.processes.append(new_proc)

    def _downscale(self):
        downscale_process = self.processes.pop(0)
        downscale_process.terminate()
        downscale_process.join()

    def _autoscale_minimum(self):
        dead = [process for process in self.processes if not process.is_alive()]

        for process in dead:
            self.processes.remove(process)

        while len(self.processes) < self.min_process_count:
            # self.logger.warning('Missing process, creating new process.')
            self._upscale()

    def autoscale(self):
        self._autoscale_minimum()


class LoadBalancer(AutoscalingGroup):
    def __init__(
        self,
        target,
        args,
        min_process_count,
        max_process_count,
        load_queue,
        max_queue_size,
    ):
        super().__init__(target, args, min_process_count)

        self.args = (load_queue, *args)
        self.load_queue = load_queue
        self.max_process_count = max_process_count
        self.max_queue_size = max_queue_size

        self._previous_queue_size = 0
        self.queue_average = 0
        self._autoscale_count = 0

    def _load_balance(self):
        current_queue_size = self.load_queue.qsize()
        queue_growth = (current_queue_size - self._previous_queue_size) > 0

        exceeded_max_queue = current_queue_size > self.max_queue_size

        if exceeded_max_queue and (self.proc_count() < self.max_process_count):
            # self.logger.debug("Max queue size exceeded.")
            self._upscale()

        if (
            not exceeded_max_queue
            and not queue_growth
            and self.proc_count() > self.min_process_count
        ):
            self._downscale()

    def _telemetry(self):
        current_queue_size = self.load_queue.qsize()
        self._previous_queue_size = current_queue_size
        self.queue_average = moving_average(
            self.queue_average, current_queue_size, self._autoscale_count
        )
        self._autoscale_count += 1

    def autoscale(self):
        self._autoscale_minimum()
        self._load_balance()
        self._telemetry()


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

    def __init__(self, db_string: str, api_config: dict):
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
    def process_datapoint(datapoint):
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
    def process_data(unprocessed_queue, latest_data, unsaved_queue):
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
    def gather_data(api_config, unprocessed_queue):
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
    def record_data(unsaved_queue, db_string):
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

    def autoscale(self):
        self.gatherers.autoscale()
        self.processors.autoscale()
        self.recorders.autoscale()

    def run(self):
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

    def stop(self):
        self.gatherers.stop()
        self.processors.stop()
        self.recorders.stop()


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
