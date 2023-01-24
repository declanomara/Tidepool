import zmq
import random
import sys
import time
import json
import pymongo
import datetime
import argparse
import logging

from dateutil import parser
from collections import deque
from threading import Thread
from pathlib import Path
from tidepool.scaling import ScalableGroup, LoadBalancer
from tidepool.compatible_queue import CompatibleQueue
from tidepool.utils import create_logger, get_multiprocessing_logger
from multiprocessing import Queue, Process, Manager
from logging.handlers import QueueHandler, QueueListener


class DBPipeline:
    def __init__(self, cfg: dict, logging_queue: Queue):
        self.logger = logging.getLogger(f'__main__.DBPipeline')
        self.ports = cfg["dataIntakePorts"]
        self.db_string = self._generate_db_string(cfg)
        self.logging_queue = logging_queue
        self.manager = Manager()

        self.data_queue = CompatibleQueue()
        self.intake_telemetry = self.manager.dict()
        self.data_intakes = [Thread(target=self.read_data, args=(port, self.data_queue, self.intake_telemetry)) for port in self.ports]

        self.processed_queue = CompatibleQueue()
        self.write_queue = CompatibleQueue()
        self.processor_telemetry = self.manager.dict()
        self.processor = LoadBalancer(
            self.process_data,
            self.data_queue,
            args=(self.processed_queue, self.write_queue, self.processor_telemetry),
            kwargs={"logging_queue": self.logging_queue},
            max_processes=16
        )

        self.recent_data = deque(maxlen=1000)
        self.deduplicator_telemetry = self.manager.dict()
        self.deduplicator = Thread(target=self.deduplicate_data, args=(self.processed_queue, self.write_queue, self.deduplicator_telemetry))

        self.recorder_telemetry = self.manager.dict()
        self.recorder = LoadBalancer(
            self.write_data,
            self.write_queue,
            args=(self.db_string, self.recorder_telemetry,),
            kwargs={"logging_queue": self.logging_queue},
            max_processes=16
        )

        # self.queue_listener = QueueListener(self.logging_queue, *logging.getLogger().handlers)

    def _generate_db_string(self, cfg: dict):
        if "dbUser" in cfg and "dbPass" in cfg:
            return f'mongodb://{cfg["dbUser"]}:{cfg["dbPass"]}@{cfg["dbHost"]}:{cfg["dbPort"]}'

        else:
            return f'mongodb://{cfg["dbHost"]}:{cfg["dbPort"]}'

    def read_data(self, port: int, queue: CompatibleQueue, telemetry: dict):
        logger = logging.getLogger(f'__main__.read_data')

        if "action_count" not in telemetry:
            telemetry["action_count"] = 0

        context = zmq.Context()
        socket = context.socket(zmq.PULL)
        socket.connect(f"tcp://localhost:{port}")

        try:
            while True:
                msg = socket.recv()
                queue.put(msg.decode("utf-8"))
                telemetry["action_count"] += 1
                logger.debug(f"Received data from port {port}.")
        
        except KeyboardInterrupt:
            socket.close()
            context.term()

    def deduplicate_data(self, processed_queue: CompatibleQueue, write_queue: CompatibleQueue, telemetry: dict):
        if "action_count" not in telemetry:
            telemetry["action_count"] = 0
        
        if "duplicate_count" not in telemetry:
            telemetry["duplicate_count"] = 0

        try:
            while True:
                db_packet = processed_queue.get()

                if db_packet not in self.recent_data:
                    self.recent_data.append(db_packet)
                    write_queue.put(db_packet)
                
                else:
                    telemetry["duplicate_count"] += 1

                telemetry["action_count"] += 1
        
        except KeyboardInterrupt:
            pass

    @staticmethod
    def process_data(data_queue: CompatibleQueue, processed_queue: CompatibleQueue, write_queue: CompatibleQueue, telemetry: dict, logging_queue: Queue = None):
        logger = get_multiprocessing_logger("process_data", logging_queue)
        
        if "action_count" not in telemetry:
            telemetry["action_count"] = 0
       
        try:
            while True:
                datapoint = data_queue.get()
                db_packet_raw = {
                    "dest": "raw",
                    "data": {
                        "time": datetime.datetime.utcnow(),
                        "data": datapoint
                    }
                }
                processed_queue.put(db_packet_raw)
                datapoint = json.loads(datapoint)

                if datapoint["type"] == "PRICE":
                    db_packet = {
                        "dest": datapoint["instrument"],
                        "data": {
                            "time": parser.parse(datapoint["time"]),
                            "bid": datapoint["closeoutBid"],
                            "ask": datapoint["closeoutAsk"],
                            "status": datapoint["status"],
                            "tradeable": datapoint["tradeable"],
                            "instrument": datapoint["instrument"],
                        }
                    }
                    
                    processed_queue.put(db_packet)
                
                elif datapoint["type"] == "HEARTBEAT":
                    logger.debug("Received heartbeat.")
                
                telemetry["action_count"] += 1

        except KeyboardInterrupt:
            pass

    @staticmethod
    def write_data(write_queue: CompatibleQueue, db_string: str, telemetry: dict, logging_queue: Queue = None):
        logger = get_multiprocessing_logger("write_data", logging_queue)

        logger.info(f"Connecting to MongoDB at {db_string}...")
        client = pymongo.MongoClient(db_string, serverSelectionTimeoutMS=1000)
        tidepooldb = client["tidepool"]
        logger.info("Connected to MongoDB.")

        if "action_count" not in telemetry:
            telemetry["action_count"] = 0

        try:
            tick = 1
            while True:
                db_packet = write_queue.get()
                destination, datapoint = db_packet["dest"], db_packet["data"]
                logger.debug(f'Writing to {destination}: {datapoint}')

                # TODO: Write to database
                try:
                    collection = tidepooldb[destination]
                    collection.insert_one(datapoint)
                
                except pymongo.errors.OperationFailure as e:
                    logger.error(f"Error writing to database: {e}")

                # End TODO

                if tick % 100 == 0:
                    logger.info(f"Written {tick} datapoints to database.")

                telemetry["action_count"] += 1
                tick += 1

        except KeyboardInterrupt:
            pass

    def start(self):
        for intake in self.data_intakes:
            intake.daemon = True
            intake.start()

        self.processor.start()
        self.deduplicator.daemon = True
        self.deduplicator.start()
        self.recorder.start()


if __name__== '__main__':
    logging_queue = CompatibleQueue()
    log_file = Path("/usr/local/tidepool/logs/terminus.log")
    create_logger(log_file, logging_queue=logging_queue)
    logger = logging.getLogger(__name__)

    installed_cfg_path = Path(f"/usr/local/tidepool/configs/terminus.json")
    relative_cfg_path = Path(f"configs/terminus.json")
    
    if installed_cfg_path.exists():
        cfg_path = installed_cfg_path
    elif relative_cfg_path.exists():
        cfg_path = relative_cfg_path
    else:
        logger.error(f"Could not find config file at {installed_cfg_path} or {relative_cfg_path}.")
        exit(1)

    cfg = json.loads(open(cfg_path).read())

    pipeline = DBPipeline(cfg, logging_queue)
    pipeline.start()

    while True:
        logger.debug(f"Data Queue: {pipeline.data_queue.qsize()}")
        logger.debug(f"Processed Queue: {pipeline.processed_queue.qsize()}")
        logger.debug(f"Write Queue: {pipeline.write_queue.qsize()}")
        logger.debug(f"Processor Telemetry: {pipeline.processor_telemetry}")
        logger.debug(f"Deduplicator Telemetry: {pipeline.deduplicator_telemetry}")
        logger.debug(f"Recorder Telemetry: {pipeline.recorder_telemetry}")
        logger.debug("")

        time.sleep(1)