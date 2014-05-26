"""processor: low level (concurrent) processing base classes"""


import time
import queue
import threading
import traceback
from abc import abstractmethod

from jarvis.config import MISC
from jarvis.utils.decorator import abstractclass


@abstractclass()
class Processor(object):
    
    """Basic linear task processor."""
     
    def __init__(self, delay=MISC.DELAY, debug=MISC.DEBUG):
        """Instantiates with custom waiting time."""

        # for multiple inheritance purposes
        super(Processor, self).__init__()
        
        self.delay = delay
        self.debug = debug    # redundant prints
        
    def _prologue(self):
        """Executed once before the main procedures."""
        pass

    def _epilogue(self):
        """Executed once after the main procedures."""
        pass
    
    @abstractmethod
    def _task_gen(self):
        """Override this with your custom task generator."""
        pass
        
    @abstractmethod
    def _process(self, task):
        """Override this with your desired procedures."""
        pass
    
    def _error(self, exc, output):
        """Generic exception handler for the main process."""
        # handle the error
        emsg = "Error: {}\n{}".format(exc, output.strip())
        print(emsg)
        
    def _refresh(self):
        """Called after each successfully processed set of tasks."""
        pass
    
    def _interrupt(self):
        """What to execute when keyboard interrupts arrive."""
        print("Stopped")
    
    def _put_task(self, task):
        """Process it."""
        try:
            self._process(task)
        except Exception as exc:
            output = traceback.format_exc()
            self._error(exc, output)
    
    def start(self):
        """Start the processor."""

        self._prologue()

        while True:
            try:
                for task in self._task_gen():
                    self._put_task(task)
                if self.debug:
                    print("Sleeping...")
                time.sleep(self.delay)
                self._refresh()
            except KeyboardInterrupt:
                self._interrupt()
                break
            
        self._epilogue()
        

@abstractclass
class ConcurrentProcessor(Processor):
    
    """Concurrent task processor."""

    STEL = None    # default sentinel
    
    def __init__(self, qsize=MISC.QSIZE, threadcount=MISC.THREADS, **kwargs):
        """Instantiates with custom number of threads and queue size."""

        super(ConcurrentProcessor, self).__init__(**kwargs)

        self.threadcount = threadcount
        self.threads = list()
        self.pqueue = queue.Queue(qsize)
        
    def _worker(self):
        while True:
            # get task
            task = self.pqueue.get()
            if task is ConcurrentProcessor.STEL:
                # reached the sentinel
                self.pqueue.task_done()
                break
            # process it
            try:
                self._process(task)
            except Exception as exc:
                output = traceback.format_exc()
                self._error(exc, output)
            # notify that the previous task was processed
            self.pqueue.task_done()
                
    def _prologue(self):
        super(ConcurrentProcessor, self)._prologue()
        # create the workers
        for _ in range(self.threadcount):
            thread = threading.Thread(target=self._worker)
            thread.start()
            self.threads.append(thread)

    def _interrupt(self):
        # signal the threads to stop
        for _ in range(self.threadcount):
            self.pqueue.put(ConcurrentProcessor.STEL)
        super(ConcurrentProcessor, self)._interrupt()
            
    def _epilogue(self):
        # wait for workers to finish
        while self.threads:
            thread = self.threads.pop()
            thread.join()
        # call it after finishing all the work
        super(ConcurrentProcessor, self)._epilogue()
    
    def _put_task(self, task):
        """Add the task to a queue instead of directly processing it."""
        if task != ConcurrentProcessor.STEL:
            self.pqueue.put(task)
