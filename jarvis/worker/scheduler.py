"""scheduler: base and custom schedulers based on daemons

Usage:
>>> from jarvis.worker.scheduler import RedisConnector, Scheduler
>>> class TaskScheduler(Scheduler):
...     def __init__(self, *args, **kargs):
...         super(TaskManager, self).__init__(*args, **kargs)
...         connection = RedisConnector("task", "job")
...         self.task_manager = TaskManager(connection)
>>> if __name__ == "__main__":
...     scheduler = TaskScheduler()
...     scheduler.start()
"""

import time
import json
import queue
import threading

from abc import abstractmethod, ABCMeta

from jarvis.config import SCHEDULER, TASK, REDIS
from jarvis.worker.dbcom import RedisConnection
from jarvis.worker.daemon import ThreadDaemon

# pylint: disable=R0913


class DatabaseConnector(object, metaclass=ABCMeta):

    """Abstract base class for database connector"""

    def __init__(self):
        """Instantiates connector."""
        self.connection = None
        self.db_lock = None
        self._connect()

    @abstractmethod
    def _connect(self):
        """Setup connection"""
        pass

    @abstractmethod
    def _delete_job(self, job):
        """Delete job from scheduler queue"""

    @abstractmethod
    def task(self, name):
        """Get information regarding received task name"""
        pass

    @abstractmethod
    def create_task(self, name, value):
        """Create a new task"""
        pass

    @abstractmethod
    def update_task(self, name, fields):
        """Update representation of task"""
        pass

    @abstractmethod
    def schedule_task(self, name, date):
        """Schedule received task to date."""
        pass

    @abstractmethod
    def task_gen(self):
        """Return all unscheduled tasks."""
        pass

    @abstractmethod
    def job_gen(self, time_window):
        """Return all scheduled jobs for next few seconds"""


class RedisConnector(DatabaseConnector):

    """Adapter for RedisConnection"""

    def __init__(self, job_key, task_key, host=REDIS.HOST,
                 port=REDIS.PORT, dbname=REDIS.DBNAME):
        """Instantiates object with custom connection data"""
        super(RedisConnector, self).__init__()
        self.host = host
        self.port = port
        self.dbname = dbname
        self.job_key = job_key
        self.task_key = task_key
        self.db_lock = threading.RLock()

    @property
    def rcon(self):
        """Getter for redis connection"""
        return self.connection.rcon

    def _connect(self):
        """Connect to Redis database"""
        self.connection = RedisConnection(self.host, self.port, self.dbname)

    def _delete_job(self, job):
        """Delete job from scheduler queue"""
        with self.db_lock:
            return self.rcon.zrem(job)

    def task(self, name):
        """Get information regarding received task name"""
        with self.db_lock:
            return self.rcon.hget(self.task_key, name)

    def create_task(self, name, value):
        """Create a new task or overwrite an existed one"""
        with self.db_lock:
            return self.rcon.hset(self.task_key, name, value)

    def update_task(self, name, fields):
        """Update representation of task"""
        task = self.task(name)
        if not task:
            return False

        try:
            data = json.loads(task)
            data.update(fields)
            task = json.dumps(data)
        except ValueError:
            return False

        return self.create_task(name, task)

    def schedule_task(self, name, date):
        """Schedule received task to date."""
        with self.db_lock:
            return self.rcon.zadd(self.job_key, name, date)

    def task_gen(self):
        """Return all unscheduled tasks."""
        tasks = []
        with self.db_lock:
            tasks = self.rcon.hgetall(self.task_key)

        # pylint: disable=E1103
        for key in list(tasks.keys()):
            yield (key, tasks.pop(key))

    def job_gen(self, time_window):
        """Return all scheduled jobs for next few seconds"""
        start = time.time()
        end = start + time_window
        jobs = []
        with self.db_lock:
            jobs = self.rcon.zrangebyscore(self.job_key, start, end)

        for job in jobs:
            task = self.task(job)
            yield (job, task)

        self._delete_job(jobs)


class Task(object):

    """Abstract base class for simple task."""

    def __init__(self, name, config, connection):
        """Instantiates task with config"""
        self.name = name or self.__class__.__name__
        self.__data = {
            "status": TASK.UNSCHEDULED,     # Task status.
            "last_run": None,               # Timestamp for last run.
            "next_run": None,               # Timestamp after task scheduled.
            "eor": None,                    # Timestamp for end of run
            "trigger_name": None,           # Name of trigger hook
            "action_name": None,            # Name of action hook
            "trigger_args": {},             # Aguments required for triger
            "action_args": {},              # Arguments required for action
        }
        self.trigger = None
        self.action = None
        self.connection = connection        # Adapter for data representation
        self.update(config)                 # Update dictionary keys

    def __str__(self):
        """String representation for current task."""
        value = "<Task: Unknown format.>"
        try:
            value = json.dumps(self.__data)
        except ValueError:
            pass
        return value

    def __repr__(self):
        """Machine-readable task representation"""
        value = "<Task: {}>".format(self.name)
        return value

    def __setattr__(self, name, value):
        """Hook set attribute method for update task representation"""
        container = getattr(self, "__data", None)
        if container and name in container:  # Check if attribute is from task
            container[name] = value          # Update task
            self.task_changed([name])        # Update task representation
            return
        self.__dict__[name] = value          # Default action

    def __getattr__(self, name):
        """Hook for getting attribute from local storage"""
        container = self.__dict__.get("_data")
        if container and name in container:
            return container[name]

        raise AttributeError("'task' object has no attribute '{}'"
                             .format(name))

    def task_changed(self, fields):
        """The task was modified."""
        update = {}
        for field in fields:
            update[field] = self.__data[field]

        self.connection.update_task(self.name, update)

    def task_done(self):
        """End of run for current task"""
        self.__data["status"] = TASK.DONE       # Set status done for task
        self.__data["eor"] = time.time()        # Update last end of run
        self.task_changed(["status", "eor"])    # Send changed event

    def start(self):
        """Task is now running"""
        self.__data["status"] = TASK.RUNNING    # Set status running dor task
        self.__data["last_run"] = time.time()   # Update last run
        self.task_changed(["status", "last_run"])  # Send changed event

    def update(self, config):
        """Update fields from local storage."""
        if not isinstance(config, dict):
            raise ValueError("config should be dictionary.")
        self.__data.update(config)

    def connect(self, scheduler):
        """Connect action and trigger to scheduler hooks"""
        trigger = getattr(scheduler, "triger_{}".format(self.trigger), None)
        action = getattr(scheduler, "action_{}".format(self.action), None)
        if not (trigger and action):
            return False        # Objects are incompatible.

        self.trigger = trigger  # Connect trigger to scheduler triger hook
        self.action = action    # Connect action to scheduler action hook
        return True

    def dump(self):
        """Text representation of task"""
        return json.dumps(self.__data)


class TaskManager(object):

    """Abstract base class for task manager"""

    def __init__(self, connection, task_class=Task):
        self.connection = connection
        self.task = task_class

    def schedule_task(self, task, date):
        """Schedule received task to date."""
        return self.connection.schedule_task(task, date)

    def tasks(self):
        """Generator for all available tasks."""
        for name, content in self.connection.task_gen():
            task = self.task(name, content, self.connection)
            yield task

    def jobs(self, time_window):
        """Generator for all scheduled tasks from waiting queue."""
        for name, content in self.connection.job_get(time_window):
            task = self.task(name, content, self.connection)
            yield task


class Scheduler(ThreadDaemon):

    """Abstract base class for simple scheduler."""

    def __init__(self, *args, **kargs):
        """Instantiates object with custom attributes specific to the
        task scheduler daemon."""
        super(Scheduler, self).__init__(*args, **kargs)
        self.running_jobs = queue.Queue()
        self.scheduler_manager = []
        self.task_manager = None

    def task_get(self):
        """Generator for all unscheduled tasks."""
        for task in self.task_manager.task():
            if task.status in (TASK.UNSCHEDULED, TASK.DONE):
                yield task

    def job_gen(self):
        """Generator for all scheduled tasks from waiting queue."""
        for job in self.task_manager.jobs():
            if job.status == TASK.SCHEDULED:
                yield job

    def put_task(self, task):
        """Adds a task to the queue."""
        # Check if current task is valid
        if not task.connect(self):  # Check if task can be used
            return                  # Drop current task
        self.queue.put(task)        # Add current task in schedule queue

    def get_task(self):
        """Retrieves a task from the queue."""
        return self.queue.get()

    def prologue(self):
        """Start parallel threads for scheduler executors."""
        super(Scheduler, self).prologue()
        for target in (self.job_supervisor, self.scheduler):
            thread = threading.Thread(target=target)
            thread.setDaemon(True)
            thread.start()
            self.scheduler_manager.append(thread)

    def epilogue(self):
        """Wait for supervisors and running jobs."""

        self.running_jobs.join()
        for supervisor in self.scheduler_manager:
            supervisor.join()

        super(Scheduler, self).epilogue()

    def process(self, task):
        """Adding task to the schedule queue."""
        # Predict timestamp for the first run
        _, date = task.trigger(wait=False, **task.trigger_args)

        # Adding the task in schedule queue
        self.task_manager.schedule_task(task, date)

    def job_supervisor(self):
        """Check running jobs for end of run event."""
        while not self.stop.is_set():
            if not self.running_jobs.empty():
                executor, job = self.running_jobs.get()
                if job.is_alive():
                    self.running_jobs.put((executor, job))
                else:
                    job.task_done()
                self.running_jobs.task_done()
            time.sleep(SCHEDULER.FINEDELAY)

    def scheduler(self):
        """Worker which starts a new executor for each job."""

        while not self.stop.is_set():
            # Getting job from the schedule queue
            for job in self.job_gen():
                executor = threading.Thread(target=self.executor, args=(job,))
                executor.start()
                self.running_jobs.put((executor, job))

            time.sleep(SCHEDULER.FINEDELAY)

    def executor(self, job):
        """Execute action when the trigger has been reached."""
        job.connect(self)
        job.trigger(wait=True, **job.trigger_args)    # Wait until job is ready
        job.start()                                   # Notify star of job
        # Run specific action for this job
        job.action(**job.action_args)
