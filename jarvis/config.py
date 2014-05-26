"""config: jarvis global settings"""


# pylint: disable=R0903, W0232, C1001

class DAEMON:

    """Global settings for Daemons."""

    PROCESSES = 1
    THREADS = 3
    SLEEP = 3


class MISC:

    """Miscellaneous settings."""

    DELAY = 10      # default delay between complete processings
    DEBUG = True    # show debugging messages or not
    THREADS = 5     # default number of threads
    QSIZE = 0       # default task processor queue size (0 - unlimited)