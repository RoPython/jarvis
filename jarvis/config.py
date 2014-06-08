"""config: jarvis global settings"""


# pylint: disable=R0903, W0232, C1001


class MISC:

    """Miscellaneous settings."""

    DEBUG = True     # show debugging messages or not


class DAEMON:

    """Daemon specific settings."""

    # default delay between complete processes and intensive iterations
    DELAY = 10
    FINEDELAY = DELAY * 0.1
    # concurrent matter
    WORKERS = 5     # default number of workers
    QSIZE = 0       # default task processor queue size (0 - unlimited)
    # other
    LOOP = False    # process the same tasks indefinitely


class DISPATCHER:

    """Dispatcher custom values."""

    # overloaded
    DELAY = 0
    LOOP = True
    # connectivity
    ADDRESS = ("127.0.0.1", 6969)


class SCHEDULER:

    """Scheduler default values."""

    # Concurrent matter
    WORKERS = 1              # Default number of workers
    DELAY = 10               # Default time between iteration
    FINEDELAY = DELAY * 0.1  # Default time between events

    # Other settings
    LOOP = True


class TASK:

    """Default values for tasks"""

    # Status for Tasks
    UNSCHEDULED = "unscheduled",
    SCHEDULED = "scheduled",
    RUNNING = "running",
    DONE = "done"
