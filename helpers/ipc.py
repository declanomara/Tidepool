import socket
import json
from .misc import create_logger


class TelemetryManager:
    def __init__(self, shared_telemetry):
        self.shared_telemetry = shared_telemetry

        self.data_stats = {
            'gatherers': {
                'total': 0,
                'rate': 0.0
            },
            'processors': {
                'total': 0,
                'rate': 0.0
            },
            'recorders': {
                'total': 0,
                'rate': 0.0
            }
        }

        self.server_stats = {}

        self.datastream_stats = {}

    def _get_data_totals(self, gatherers, processors, recorders) -> tuple:
        total_data_gathered = gatherers.telemetry["action_count"]
        total_data_processed = processors.telemetry["action_count"]
        total_data_recorded = recorders.telemetry["action_count"]

        return total_data_gathered, total_data_processed, total_data_recorded

    def _calculate_rates(self, gatherers, processors, recorders, interval):
        (
            total_data_gathered,
            total_data_processed,
            total_data_recorded,
        ) = self._get_data_totals(gatherers, processors, recorders)

        data_gather_rate: float = (
            total_data_gathered - self.data_stats["gatherers"]["total"]
        ) / interval
        data_process_rate: float = (
            total_data_processed - self.data_stats["processors"]["total"]
        ) / interval
        data_record_rate: float = (
            total_data_recorded - self.data_stats["recorders"]["total"]
        ) / interval

        return data_gather_rate, data_process_rate, data_record_rate

    def update_data_stats(self, gatherers, processors, recorders, interval):
        data_gathered, data_processed, data_recorded = self._get_data_totals(
            gatherers, processors, recorders
        )
        gather_rate, process_rate, record_rate = self._calculate_rates(
            gatherers, processors, recorders, interval
        )

        self.data_stats['gatherers'] = {'total': data_gathered, 'rate': gather_rate}
        self.data_stats['processors'] = {'total': data_processed, 'rate': process_rate}
        self.data_stats['recorders'] = {'total': data_recorded, 'rate': record_rate}

    def update_server_stats(self, gatherers, processors, recorders, unsaved_queue, unprocessed_queue, uptime):
        self.server_stats['proc_counts'] = {
            'gatherers': gatherers.proc_count(),
            'processors': processors.proc_count(),
            'recorders': recorders.proc_count()
        }

        self.server_stats['queues'] = {
            'unprocessed': {
                'size': unprocessed_queue.qsize(),
                'average': processors.queue_average
            },
            'unsaved': {
                'size': unsaved_queue.qsize(),
                'average': recorders.queue_average
            }
        }

        self.server_stats['uptime'] = uptime

    def update_instrument_stats(self, processors, interval):
        for instrument, count in processors.telemetry.items():
            if instrument == 'action_count':
                continue

            rate = 0
            if instrument in self.datastream_stats:
                previous = self.datastream_stats[instrument]['count']
                rate = (count - previous) / interval

            self.datastream_stats[instrument] = {
                'count': count,
                'rate': rate
            }

    def update_shared_telemetry(self):
        self.shared_telemetry['data'] = self.data_stats
        self.shared_telemetry['server'] = self.server_stats
        self.shared_telemetry['datastream'] = self.datastream_stats


telemetry_format = {
    "data": {
        "gatherers": {"total": -1, "rate": -1.0},
        "processors": {"total": -1, "rate": -1.0},
        "recorders": {"total": -1, "rate": -1.0},
    },
    "server": {
        "proc_counts": {"gatherers": -1, "processors": -1, "recorders": -1},
        "queues": {
            "unprocessed": {"size": -1, "average": -1},
            "unsaved": {"size": -1, "average": -1},
        },
        "uptime": -1,
    },
}


def expose_telemetry(exposed_telemetry: dict, telemetry: dict = None) -> None:
    logger = create_logger()
    host = "127.0.0.1"
    port = 65001  # TODO: Auto-assign port to avoid collision with other software

    server_socket = socket.socket()
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))  # bind host address and port together
    logger.info(f"Successfully created socket listening on {host}:{port}")

    # Configure how many client the server can listen simultaneously
    server_socket.listen(10)
    while True:
        logger.info("Waiting for client...")
        conn, address = server_socket.accept()

        while True:
            # Data does not matter, server always responds with telemetry data
            data = conn.recv(1024).decode()
            if not data:
                break

            response = json.dumps(
                exposed_telemetry.copy()
            )  # Must copy as shared dict is not JSON serializable
            conn.send(response.encode("utf-8"))
            logger.info("Served telemetry data.")

        if telemetry is not None:
            telemetry["action_count"] += 1

        conn.close()
