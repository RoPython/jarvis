"""daemon: server-like task scheduler and processor"""


import time
import queue
import threading
import multiprocessing

from abc import abstractmethod, ABCMeta

from jarvis.config import MISC, DAEMON
from jarvis.util.misc import get_uid


class Daemon(object, metaclass=ABCMeta):

    """Abstract base class for simple daemons."""

    def __init__(self, name=None, debug=MISC.DEBUG,
                 delay=DAEMON.DELAY, loop=DAEMON.LOOP):
        """Setup a new instance."""
        # refine name
        if not name:
            name = "{}_{}".format(self.__class__.__name__, get_uid())

        # save attributes
        self.name = name
        self.debug = debug
        self.delay = delay
        self.loop = loop

    def prologue(self):
        """Executed once before the main procedures."""
        pass

    def epilogue(self):
        """Executed once after the main procedures."""
        pass

    @abstractmethod
    def task_generator(self):
        """Override this with your custom task generator."""
        pass

    @abstractmethod
    def process(self, task):
        """Override this with your desired procedures."""
        pass

    def _process(self, task):
        """Wrapper over the `process`."""
        # pylint: disable=W0703
        try:
            result = self.process(task)
        except Exception as exc:
            self.task_fail(task, exc)
        else:
            self.task_done(task, result)

    def put_task(self, task):
        """Adds the task in the queue."""
        self._process(task)

    def task_done(self, task, result):
        """What to execute after successfully finished processing a task."""
        pass

    def task_fail(self, task, exc):
        """What to do when the program fails processing a task."""
        pass

    def finished(self):
        """What to execute after finishing processing all the tasks."""
        return self.loop    # decide to continue reprocessing or not

    def interrupted(self):
        """What to execute when keyboard interrupts arrive."""
        pass

    def start(self):
        """Starts a series of workers and processes incoming tasks."""
        self.prologue()
        while True:
            try:
                for task in self.task_generator():
                    self.put_task(task)
                loop = self.finished()
                if not loop:
                    break
                time.sleep(self.delay)
            except KeyboardInterrupt:
                self.interrupted()
                break
        self.epilogue()


class _ConcurrentDaemon(Daemon):

    """Abstract base class for concurrent daemons.

    Not inherited directly into final classes.
    """

    def __init__(self, *args, qsize=DAEMON.QSIZE,
                 wcount=DAEMON.WORKERS, **kwargs):
        """Partial setup the new concurrent instance."""
        super(_ConcurrentDaemon, self).__init__(*args, **kwargs)

        self.qsize = qsize       # maximum allowed queue size
        self.wcount = wcount     # desired number of workers

        self.workers = list()    # workers as objects
        self.manager = None      # who supervises the workers
        self.queue = None        # processing queue
        self.stop = None         # event telling that all the things must end

    @abstractmethod
    def start_worker(self):
        """Create a custom worker (thread/process) and return its object."""
        pass

    def manage_workers(self):
        """Maintain a desired number of workers up."""

        while not self.stop.is_set():
            for worker in self.workers[:]:
                if not worker.is_alive():
                    self.workers.remove(worker)

            if len(self.workers) == self.wcount:
                time.sleep(DAEMON.FINEDELAY)
                continue

            worker = self.start_worker()
            self.workers.append(worker)

    def prologue(self):
        """Start a parallel supervisor."""
        super(_ConcurrentDaemon, self).prologue()

        self.manager = threading.Thread(target=self.manage_workers)
        self.manager.start()

    def epilogue(self):
        """Wait for that supervisor and its workers."""

        self.manager.join()
        for worker in self.workers:
            if worker.is_alive():
                worker.join()

        super(_ConcurrentDaemon, self).epilogue()

    def interrupted(self):
        """Mark the processing as stopped."""
        self.stop.set()
        super(_ConcurrentDaemon, self).interrupted()

    def put_task(self, task):
        """Adds a task to the queue."""
        self.queue.put(task)

    def get_task(self):
        """Retrieves a task from the queue."""
        return self.queue.get()

    def task_done(self, task, result):
        self.queue.task_done()
        super(_ConcurrentDaemon, self).task_done(task, result)

    def work(self):
        """Worker able to retrieve and process tasks."""
        while not self.stop.is_set():
            task = self.get_task()
            self._process(task)


class ThreadDaemon(_ConcurrentDaemon):

    """Abstract base class for concurrent daemons with thread workers."""

    def __init__(self, *args, **kwargs):
        """Instantiates with custom number thread safe objects."""
        super(ThreadDaemon, self).__init__(*args, **kwargs)

        self.queue = queue.Queue(self.qsize)
        self.stop = threading.Event()

    def start_worker(self):
        """Creates a new thread."""
        worker = threading.Thread(target=self.work)
        worker.setDaemon(True)
        worker.start()
        return worker


class ProcessDaemon(_ConcurrentDaemon):

    """Abstract base class for concurrent daemons with process workers."""

    def __init__(self, *args, **kwargs):
        """Instantiates with custom number of processes."""
        super(ProcessDaemon, self).__init__(*args, **kwargs)

        self.queue = multiprocessing.JoinableQueue()
        self.stop = multiprocessing.Event()

    def start_worker(self):
        """Creates a new process."""
        worker = multiprocessing.Process(target=self.work)
        worker.start()
        return worker
