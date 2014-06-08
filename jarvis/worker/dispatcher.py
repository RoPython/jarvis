"""dispatcher: base and custom dispatchers based on daemons"""


import sys
import logging
from collections import namedtuple
from multiprocessing.connection import Listener

from jarvis.config import DISPATCHER
from jarvis.worker.daemon import ThreadDaemon


class Dispatcher(ThreadDaemon):

    """Base class for dispatching."""

    def __init__(self, *args, listener=None, delay=DISPATCHER.DELAY,
                 loop=DISPATCHER.LOOP, **kwargs):
        """Init with custom values and take care of `listener` (server)."""
        super(Dispatcher, self).__init__(*args, delay=delay, loop=loop,
                                         **kwargs)

        if not listener:
            listener = Listener(DISPATCHER.ADDRESS)

        self.listener = listener
        self.logger = logging.getLogger(self.name)

    def prologue(self):
        """Setup logger."""
        super(Dispatcher, self).prologue()

        formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)

        self.logger.addHandler(stream_handler)
        if self.debug:
            self.logger.setLevel(logging.DEBUG)

    def epilogue(self):
        """Close server connection."""
        self.listener.close()
        super(Dispatcher, self).epilogue()

    def task_generator(self):
        """Listens for new requests (connections)."""
        # pylint: disable=W0703

        try:
            task = self.listener.accept()
            if task:
                yield task
        except Exception as exc:
            if str(exc).find("The pipe is being closed") == -1:
                self.logger.error(exc)

    def put_task(self, task):
        """Pre-process the task as a new connection then add it in the queue."""

        try:
            _dict = task.recv()
        except EOFError:
            self.logger.error("The pipe is being closed")
            return
        except Exception as exc:
            self.logger.error("Error occurred while receiving: {}".format(exc))
            return

        query = namedtuple("Query", _dict.keys())
        data = query(*_dict.values())

        super(Dispatcher, self).put_task((data, task, _dict))

    def process(self, task):
        """Treats a preprocessed request."""

        info = task[0]
        try:
            function = "handle_{}".format(info.request)
        except AttributeError:
            raise ValueError("Missing the request field from request")

        try:
            func = getattr(self, function)
        except AttributeError:
            raise ValueError("Function {} not defined".format(function))

        try:
            response = func(task)
        except Exception as exc:
            raise Exception(str(exc))

        return response

    def task_done(self, task, result):
        """Sends the response back."""
        try:
            task[1].send(result)
        except EOFError as exc:
            self.logger.error(exc)
        except Exception as exc:
            self.logger.error(exc)

        super(Dispatcher, self).task_done(task, result)

    def task_fail(self, task, exc):
        self.logger.error(exc)
        super(Dispatcher, self).task_fail(task, exc)
