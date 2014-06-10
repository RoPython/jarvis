"""dbcom: database communication support"""

import redis
from jarvis.config import REDIS, MISC


class RedisConnection(object):

    """High level wrapper over the redis data structures operations."""

    def __init__(self, host=REDIS.HOST, port=REDIS.PORT, db=REDIS.DBNAME):
        """Instantiates objects able to store and retrieve data."""
        self.__rcon = None
        self._host, self._port, self._db = host, port, db
        self.refresh()

    def _connect(self):
        """Try establishing a connection until succeeds."""
        try:
            rcon = redis.StrictRedis(self._host, self._port, self._db)
            # return the connection only if is valid and reachable
            if not rcon.ping():
                return None
        except (redis.ConnectionError, redis.RedisError) as _:
            return None
        return rcon

    def refresh(self, tries=MISC.TRIES):
        """Re-establish the connection only if is dropped."""
        for _ in range(tries):
            if not self.__rcon or not self.__rcon.ping():
                self.__rcon = self._connect()
            else:
                break
        else:
            return False

        return True

    @property
    def rcon(self):
        """Return a Redis connection."""
        self.refresh()
        return self.__rcon
