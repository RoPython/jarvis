"""daemon: server-like task scheduler and processor"""
import sys
import time
import queue
import logging
import threading
import multiprocessing

from abc import abstractmethod
from collections import namedtuple

from jarvis.config import MISC
from jarvis.utils.decorator import abstractclass

# pylint: disable=W0223


@abstractclass()
class Daemon(object):

    """Abstract base class for daemons."""

    def __init__(self, **kargs):
        """Setup new instance"""

        # for multiple inheritance purposes
        super(Daemon, self).__init__(**kargs)
        self.delay = kargs.get("delay", MISC.DELAY)
        self.name = kargs.get("name", self.__class__.__name__)

    def _prologue(self):
        """Executed once before the main procedures."""
        pass

    @abstractmethod
    def _task_gen(self):
        """Override this with your custom task generator."""
        pass

    @abstractmethod
    def process(self, task):
        """Override this with your desired procedures."""
        pass

    def _epilogue(self):
        """Executed once after the main procedures."""
        pass

    def _interrupt(self):
        """What to execute when keyboard interrupts arrive."""
        pass

    def serve(self):
        """Starts a series of workers and listens for new requests."""
        self._prologue()
        while True:
            try:
                for task in self._task_gen():
                    self.process(task)
            except KeyboardInterrupt:
                self._interrupt()
                break
            time.sleep(self.delay)
        self._epilogue()


@abstractclass()
class ConcurrentDaemon(Daemon):

    """Abstract base class for concurrent daemons."""

    def __init__(self, **kargs):
        """Instantiates with custom number of arguments for subclasses."""
        super(ConcurrentDaemon, self).__init__(**kargs)
        self.workers = []
        self.manager = None
        self.count = kargs.get("threads", MISC.THREADS)

    @abstractmethod
    def _put_task(self, task):
        """Adding the task in the queue."""
        pass

    @abstractmethod
    def _get_task(self):
        """Getting the task from the queue."""
        pass

    @abstractmethod
    def _task_done(self, task, result=None):
        """Indicate that a formerly enqueued task is complete."""
        pass

    def worker(self):
        """Worker that gets taks from the queue and calls process."""
        while True:
            task = self._get_task()
            result = self.process(task)
            self._task_done(task, result)

    def serve(self):
        """Starts a series of workers and listens for new requests."""
        self._prologue()
        while True:
            try:
                for task in self._task_gen():
                    self.process(task)
            except KeyboardInterrupt:
                self._interrupt()
                break
            time.sleep(self.delay)
        self._epilogue()


@abstractclass()
class ThreadConcurrentDaemon(ConcurrentDaemon):

    """Abstract base class for concurrent daemons with thread workers."""

    def __init__(self, **kargs):
        """Instantiates with custom number of threads."""
        super(ThreadConcurrentDaemon, self).__init__(**kargs)
        self.queue = queue.Queue(kargs.get("qzise", MISC.QSIZE))
        self.stop = threading.Event()

    def wakeup_workers(self):
        """Wake up all the processes required."""
        while not self.stop.is_set():
            for thread in self.workers[:]:
                if not thread.is_alive():
                    self.workers.remove(thread)

            if len(self.workers) == self.count:
                time.sleep(MISC.DELAY)
                continue

            thread = threading.Thread(target=self.worker)
            thread.setDaemon(True)
            thread.start()
            self.workers.append(thread)

    def _put_task(self, task):
        """Adding the task in the queue."""
        self.queue.put(task, block=True)

    def _get_task(self):
        """Getting the task from the queue."""
        item = self.queue.get(block=True)
        return item

    def _task_done(self, task, result=None):
        """Indicate that a formerly enqueued task is complete."""
        self.queue.task_done()

    def _prologue(self):
        """Executed once before the main procedures."""
        super(ThreadConcurrentDaemon, self)._prologue()
        self.manager = threading.Thread(target=self.wakeup_workers)
        self.manager.start()

    def worker(self):
        """Worker that gets taks from the queue and calls process"""
        while not self.stop.is_set():
            task = self._get_task()
            result = self.process(task)
            self._task_done(task, result)

    def _epilogue(self):
        """Executed once after the main procedures."""
        self.stop.set()
        self.manager.join()
        for thread in self.workers:
            if thread.is_alive():
                thread.join()

        super(ThreadConcurrentDaemon, self)._epilogue()


@abstractclass()
class ProcessConcurrentDaemon(ConcurrentDaemon):

    """Abstract base class for concurrent daemons with process workers."""

    def __init__(self, **kargs):
        """Instantiates with custom number of processes."""
        super(ProcessConcurrentDaemon, self).__init__(**kargs)
        self.queue = multiprocessing.JoinableQueue(
            kargs.get("qzise", MISC.QSIZE))
        self.stop = multiprocessing.Event()

    def wakeup_workers(self):
        """Wake up all the processes required."""
        while not self.stop.is_set():
            for process in self.workers[:]:
                if not process.is_alive():
                    self.workers.remove(process)

            if len(self.workers) == self.count:
                time.sleep(MISC.DELAY)
                continue

            process = multiprocessing.Process(target=self.worker)
            process.start()
            self.workers.append(process)

    def _put_task(self, task):
        """Adding the task in the queue."""
        self.queue.put(task, block=True)

    def _get_task(self):
        """Getting the task from the queue."""
        item = self.queue.get(block=True)
        return item

    def _task_done(self, task, result=None):
        """Indicate that a formerly enqueued task is complete."""
        self.queue.task_done()

    def _prologue(self):
        """Executed once before the main procedures."""
        super(ProcessConcurrentDaemon, self)._prologue()
        self.manager = multiprocessing.Process(target=self.wakeup_workers)
        self.manager.start()

    def worker(self):
        """Worker that gets taks from the queue and calls process"""
        while not self.stop.is_set():
            task = self._get_task()
            result = self.process(task)
            self._task_done(task, result)

    def _epilogue(self):
        """Executed once after the main procedures."""
        self.stop.set()
        self.manager.join()
        for process in self.workers:
            if process.is_alive():
                process.join()

        super(ProcessConcurrentDaemon, self)._epilogue()


@abstractclass()
class Dispatcher(ThreadConcurrentDaemon):

    """Abstract base class for dispatcher daemons."""

    def __init__(self, **kargs):
        super(Dispatcher, self).__init__(**kargs)
        self.listener = None
        self.logger = logging.getLogger(self.name)

    def _task_gen(self):
        """Listens for new requests."""
        # pylint: disable=W0703

        while not self.stop.is_set():
            try:
                task = self.listener.accept()
                if task:
                    yield task
            except Exception as exc:
                if not str(exc).find("The pipe is being closed") > -1:
                    self.logger.error(exc)

    def _get_task(self):
        """Getting the task from the queue."""
        item = self.queue.get(block=True)
        return item

    def _put_task(self, task):
        """Adding the task in the queue."""
        try:
            _dict = task.recv()
        except EOFError as exc:
            self.logger.error("The pipe is being closed")
            return
        except Exception as exc:
            self.logger.error("Error occured while recv-ing: {}".format(exc))
            return

        query = namedtuple('Query', _dict.keys())
        data = query(*_dict.values())
        # Adding in queue a nametuple, client instance and original content
        self.queue.put((data, task, _dict), block=True)

    def _task_done(self, task, result=None):
        """Indicate that a formerly enqueued task is complete."""
        self.queue.task_done()
        try:
            task[1].send(result)
        except (ValueError, EOFError) as exc:
            self.logger.error(exc)

    def _prologue(self):
        """Setup logger"""
        super(Dispatcher, self)._prologue()
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)
        self.logger.setLevel(logging.DEBUG)

    def _epilogue(self):
        """Executed once before the main procedures."""
        self.listener.close()
        super(Dispatcher, self)._epilogue()

    def process(self, task):
        """Treats a request."""
        info = task[0]
        try:
            function = 'handle_{}'.format(info.request)
        except AttributeError:
            raise ValueError("Missing the request field from request.")

        try:
            func = getattr(self, function)
        except AttributeError:
            raise ValueError("Function {} not defined!".format(function))

        try:
            response = func(task)
        except Exception as exc:
            raise ValueError(str(exc))

        return response

    def worker(self):
        """Worker that gets taks from the queue and calls process"""
        while not self.stop.is_set():
            task = self._get_task()
            try:
                result = self.process(task)
            except ValueError as exc:
                result = str(exc)
            self._task_done(task, result)


if __name__ == "__main__":
    print("This module was not designed to be used in this way.")
