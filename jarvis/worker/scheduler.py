"""scheduler: base and custom schedulers based on daemons"""
import time
import queue
import threading

from abc import abstractmethod

from jarvis.config import SCHEDULER, TASK
from jarvis.worker.daemon import ThreadDaemon


class Scheduler(ThreadDaemon):

    """Abstract base class for simple scheduler."""

    def __init__(self, *args, **kargs):
        """Instantiates object with custom attributes specific to the
        task scheduler daemon."""
        super(Scheduler, self).__init__(*args, **kargs)
        self.scheduler_manager = []
        self.running_jobs = queue.Queue()

    @abstractmethod
    def schedule_task(self, task, date):
        """Schedule received task to date."""
        pass

    @abstractmethod
    def set_status(self, task, status):
        """Update status for a specific task."""
        pass

    @abstractmethod
    def job_gen(self):
        """Job generator which will return all unscheduled tasks."""
        pass

    def put_task(self, task):
        """Adds a task to the queue."""
        # Check if current task is valid
        for method in ("action", "trigger"):
            value = task.get(method, None)
            if not hasattr(self, "{}_{}".format(method, value)):
                return False    # Drop current task
        self.queue.put(task)    # Add current task in schedule queue

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
        # Getting trigger for this task
        trigger = getattr(self, "triger_{}".format(task["trigger"]))
        # Predict timestamp for the first run
        _, date = trigger(wait=False, **task["trigger"]["args"])

        self.schedule_task(task, date)  # Adding the task in schedule queue
        self.set_status(task["name"], TASK.SCHEDULED)  # Update task status

    def job_supervisor(self):
        """Check running jobs for end of run event."""
        while not self.stop.is_set():
            if not self.running_jobs.empty():
                executor, job = self.running_jobs.get()
                if job.is_alive():
                    self.running_jobs.put((executor, job))
                else:
                    self.set_status(job, TASK.DONE)
                self.running_jobs.task_done()
            time.sleep(SCHEDULER.FINEDELAY)

    def scheduler(self):
        """Worker which starts a new executor for each job."""

        while not self.stop.is_set():
            # Getting job from the schedule queue
            for job in self.job_gen():
                executor = threading.Thread(target=self.executor, args=(job,))
                executor.start()
                self.running_jobs.put((executor, job["name"]))

            time.sleep(SCHEDULER.FINEDELAY)

    def executor(self, job):
        """Execute action when the trigger has been reached."""

        trigger = getattr(self, "triger_{}".format(job["trigger"]))
        action = getattr(self, "action_{}".format(job["action"]))

        self.set_status(job["name"], TASK.RUNNING)
        trigger(job, wait=True)    # Wait until job is ready
        action(**job.get("args"))  # Run specific action for this job
