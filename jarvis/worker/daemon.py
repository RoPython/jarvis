"""daemon: server-like task scheduler and processor"""


import time
from abc import abstractmethod
from threading import Thread, Event


from jarvis.config import DAEMON
from jarvis.utils.decorator import abstractclass


@abstractclass()
class Daemon(object):

    """Abstract base class for daemons."""

    def __init__(self):
        """Setup new instance"""

        # for multiple inheritance purposes
        super(Daemon, self).__init__()

        self.stop = Event()
        self.delay = 0.5

    @abstractmethod
    def _task_gen(self):
        """Override this with your custom task generator."""

    @abstractmethod
    def _put_task(self, task):
        """Adding the task in the queue."""

    @abstractmethod
    def _get_task(self):
        """Getting the task from the queue."""

    @abstractmethod
    def _process(self, task):
        """Override this with your desired procedures."""

    def _prologue(self):
        """Executed once before the main procedures."""
        pass

    def _epilogue(self):
        """Executed once after the main procedures."""
        pass

    def _interrupt(self):
        """What to execute when keyboard interrupts arrive."""
        pass

    def worker(self):
        """Worker that gets taks from the queue and calls _process"""
        while True:
            task = self._get_task()
            self._process(task)

    def serve(self):
        """Starts a series of workers and listens for new requests."""

        self._prologue()

        while not self.stop.is_set():
            try:
                for request in self._task_gen():
                    self._put_task(request)
                time.sleep(self.delay)
            except KeyboardInterrupt:
                self._interrupt()
                break

        self._epilogue()


@abstractclass()
class ConcurrentDaemon(Daemon):

    """Abstract base class for concurrent daemons."""

    def __init__(self, **kargs):
        """Instantiates with custom number of threads."""
        super(ConcurrentDaemon, self).__init__()
        self.__data = {
            "threads": kargs.get("threads", DAEMON.THREADS),
            "name": kargs.get("name", self.__class__.__name__),
            "workers": [],
            "manager": None
        }

    def wakeup_workers(self):
        """Wake up all the processes required."""
        while not self.stop.is_set():
            for thread in self.__data["workers"][:]:
                if not thread.is_alive():
                    self.__data["workers"].remove(thread)

            if len(self.__data["workers"]) == self.__data["threads"]:
                time.sleep(DAEMON.SLEEP)
                continue

            thread = Thread(target=self.worker)
            thread.setDaemon(True)
            thread.start()
            self.__data["workers"].append(thread)

    def _prologue(self):
        """Executed once before the main procedures."""
        super(ConcurrentDaemon, self)._prologue()
        self.__data["manager"] = Thread(target=self.wakeup_workers)
        self.__data["manager"].start()

    def _epilogue(self):
        """Executed once after the main procedures."""
        self.stop.set()
        self.__data["manager"].join()
        for thread in self.__data["workers"]:
            if thread.is_alive():
                thread.join()

        super(ConcurrentDaemon, self)._epilogue()


if __name__ == "__main__":
    print("This module was not designed to be used in this way.")
