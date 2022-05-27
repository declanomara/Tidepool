from .misc import moving_average
from multiprocessing import Process
from queue import Queue
from typing import Callable


class AutoscalingGroup:
    def __init__(self, target: Callable, args: tuple, min_process_count: int) -> None:
        """
        :param target: Callable object to run in parallel
        :param args: Arguments to pass to target
        :param min_process_count: Minimum number of processes to maintain simultaneously
        :rtype: None
        """
        self.target = target
        self.args = args
        self.min_process_count = min_process_count

        self.processes = []

    def refresh_procs(self) -> None:
        """
        :rtype: None
        """
        for _ in range(self.proc_count()):
            self._downscale()
            self._upscale()

    def _create_proc(self) -> Process:
        """
        :return: Returns a multiprocessing Process instance
        :rtype: Process
        """
        process = Process(target=self.target, args=self.args)
        process.daemon = True
        return process

    def stop(self) -> None:
        """
        :rtype: None
        """
        while self.proc_count() > 0:
            self._downscale()

    def start(self) -> None:
        """
        :rtype: None
        """
        for _ in range(self.min_process_count):
            process = self._create_proc()
            self.processes.append(process)

        for process in self.processes:
            process.start()

    def proc_count(self) -> int:
        """
        :return: Returns number of active processes
        :rtype: int
        """
        return len(self.processes)

    def _upscale(self) -> None:
        """
        :rtype: None
        """
        new_proc = self._create_proc()
        new_proc.start()
        self.processes.append(new_proc)

    def _downscale(self) -> None:
        """
        :rtype: None
        """
        downscale_process = self.processes.pop(0)
        downscale_process.terminate()
        downscale_process.join()

    def _autoscale_minimum(self) -> None:
        """
        :rtype: None
        """
        dead = [process for process in self.processes if not process.is_alive()]

        for process in dead:
            self.processes.remove(process)

        while len(self.processes) < self.min_process_count:
            # self.logger.warning('Missing process, creating new process.')
            self._upscale()

    def autoscale(self) -> None:
        """
        :rtype: None
        """
        self._autoscale_minimum()


class LoadBalancer(AutoscalingGroup):
    def __init__(
        self,
            target: Callable,
            args: tuple,
            min_process_count: int,
            max_process_count: int,
            load_queue: Queue,
            max_queue_size: int,
    ) -> None:
        """
        :param target: Callable object to run in parallel
        :param args: Arguments to pass to target object
        :param min_process_count: Minimum number of processes to maintain simultaneously
        :param max_process_count: Maximum number of processes to maintain simultaneously
        :param load_queue: Queue object that contains load objects for target object
        :param max_queue_size: Maximum number of load objects in queue before increasing pool size
        :rtype: None
        """
        super().__init__(target, args, min_process_count)

        self.args = (load_queue, *args)
        self.load_queue = load_queue
        self.max_process_count = max_process_count
        self.max_queue_size = max_queue_size

        self._previous_queue_size = 0
        self.queue_average = 0
        self._autoscale_count = 0

    def _load_balance(self) -> None:
        """
        :rtype: None
        """
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

    def _telemetry(self) -> None:
        """
        :rtype: None
        """
        current_queue_size = self.load_queue.qsize()
        self._previous_queue_size = current_queue_size
        self.queue_average = moving_average(
            self.queue_average, current_queue_size, self._autoscale_count
        )
        self._autoscale_count += 1

    def autoscale(self) -> None:
        """
        :rtype: None
        """
        self._autoscale_minimum()
        self._load_balance()
        self._telemetry()
