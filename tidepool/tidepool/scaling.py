from multiprocessing import Process, Queue
from typing import Callable


class ScalableGroup:
    def __init__(self, target: Callable, args: tuple = (), kwargs: dict = {}, min_processes: int = 1, max_processes: int = 1):
        self.target = target
        self.args = args
        self.kwargs = kwargs
        self.max_processes = max_processes if max_processes > min_processes else min_processes
        self.min_processes = min_processes
        self.processes = []

        self.is_running = False

    def _create_process(self):
        process = Process(target=self.target, args=self.args, kwargs=self.kwargs)
        process.daemon = True

        return process

    def _scale_up(self):
        process = self._create_process()
        process.start()
        self.processes.append(process)

    def _scale_down(self):
        process = self.processes.pop()
        process.terminate()
        process.join()

    def _prune(self):
        dead = [process for process in self.processes if not process.is_alive()]

        for process in dead:
            self.processes.remove(process)

    def _scale(self):
        while self.process_count() < self.min_processes:
            self._scale_up()

        while self.process_count() > self.max_processes:
            self._scale_down()

    def __repr__(self) -> str:
        return f"ScalableGroup({self.target}, {self.args}, {self.kwargs}, {self.min_processes}, {self.max_processes})"

    def process_count(self):
        return len(self.processes)

    def autoscale(self):
        if not self.is_running:
            raise Exception('Cannot autoscale a stopped group. Did you forget to call start()?')

        self._prune()
        self._scale()

    def refresh(self):
        for _ in range(self.process_count()):
            self._scale_down()
            self._scale_up()

    def start(self):
        self.is_running = True
        for _ in range(self.min_processes):
            process = self._create_process()
            process.start()
            self.processes.append(process)
    
    def stop(self):
        while self.processes:
            self._scale_down()


class LoadBalancer(ScalableGroup):
    def __init__(self, target: Callable, load_queue: Queue, max_queue_size: int = 100, args: tuple = (), kwargs: dict = {}, min_processes: int = 1, max_processes: int = 1):
        super().__init__(target, args, kwargs, min_processes, max_processes)

        self.args = (load_queue, *args)

        self.load_queue = load_queue
        self.max_queue_size = max_queue_size

        self.previous_queue_size = 0

    def _balance_load(self):
        queue_size = self.load_queue.qsize()
        queue_shrinking = self.load_queue.qsize() < self.previous_queue_size

        if queue_size > self.max_queue_size and len(self.processes) < self.max_processes:
            self._scale_up()

        elif queue_size < self.max_queue_size and queue_shrinking and len(self.processes) > self.min_processes:
            self._scale_down()

        self.previous_queue_size = queue_size

    def __repr__(self) -> str:
        return super().__repr__() + f" - Load: {self.load_queue.qsize()}"

    def autoscale(self):
        super().autoscale()
        self._balance_load()

    def queue_size(self):
        return self.load_queue.qsize()


