"""daemon: server-like task scheduler and processor"""


import time
import signal
from threading import Thread, Event

from jarvis.config import DAEMON


class Daemon(object):

    """Base model for daemons."""

    def __init__(self, **kargs):
        """Instantiates with custom settings."""
        self.__data = {
            "threads": kargs.get("threads", DAEMON.THREADS),
            "name": kargs.get("name", self.__class__.__name__),
            "workers": [],
        }
        self.supervisor = None
        self.stop = Event()

    def worker(self):
        """Worker that gets tasks from a queue and calls dispatcher."""
        while not self.stop.is_set():
            # TODO: Implement the worker
            pass

    def wakeup_workers(self):
        """Wake up all the processes required."""
        while not self.stop.is_set():
            for thread in self.__data["workers"][:]:
                if not thread.is_alive():
                    self.__data["workers"].remove(thread)

            if len(self.__data["workers"]) == self.__data["threads"]:
                time.sleep(DAEMON.SLEEP)
                continue

            thread = Thread(name="{}_{}".format(self.__data["name"],
                                                len(self.__data["workers"])),
                            target=self.worker)
            thread.setDaemon(True)
            thread.start()
            self.__data["workers"].append(thread)

    def cleanup(self, signum, frame):
        """Shutdown all daemons."""
        self.stop.set()
        self.supervisor.join()

        for thread in self.__data["workers"]:
            if thread.is_alive():
                thread.join()

    def start(self):
        """Inkove the daemon."""

        self.supervisor = Thread(name='supervisor', target=self.wakeup_workers)
        self.supervisor.start()
        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, self.cleanup)
        signal.pause()


if __name__ == "__main__":
    print("This module was not designed to be used in this way.")
