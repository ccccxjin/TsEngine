import queue
import threading
import time

from ..exc import InvalidOperationError
from ..util.connection import make_ts_connect


class QueuePool:
    _available = True
    _mutex = threading.Lock()
    can_put = threading.Condition(_mutex)
    can_get = threading.Condition(_mutex)

    def __init__(self, maxsize=5, wait=True, db_info=None):
        self.maxsize = maxsize
        self.wait = wait
        self.db_info = db_info

        self.queue = queue.Queue(maxsize)
        self.checkout = 0
        self._init()

    def _init(self):
        self.queue.put(self._new_connection())

    def get(self):
        if not self._available:
            raise InvalidOperationError('pool not available')
        with self.can_get:
            if self.busy:
                if not self.wait:
                    raise TimeoutError('get session timeout error')
                if self.wait is True:
                    while self.busy:
                        self.can_get.wait()
                else:
                    end_time = time.time() + self.wait
                    while self.busy:
                        remaining = end_time - time.time()
                        if remaining <= 0.0:
                            raise TimeoutError('get session timeout error')
                        self.can_get.wait(remaining)
            try:
                connection = self.queue.get(block=False)
            except queue.Empty:
                connection = self._new_connection()
            self.can_put.notify()
            self.checkout += 1
            return connection

    def put(self, con):
        if not self._available:
            con.close()
            return
        with self.can_put:
            self.queue.put(con)
            self.can_get.notify()
            self.checkout -= 1

    def detach(self):
        self._available = False
        while not self.queue.empty():
            _con = self.queue.get(block=False)
            _con.close()

    def _new_connection(self, con=None):
        if not self._available:
            raise InvalidOperationError('pool not available')
        if not con:
            con = make_ts_connect(**self.db_info)
        return con

    @property
    def busy(self):
        return self.checkedout >= self.maxsize

    @property
    def checkedin(self):
        return self.queue.qsize()

    @property
    def checkedout(self):
        return self.checkout

    @property
    def state(self):
        return (
                "Maxsize: %d,  Checkedin: %d,  Checkedout: %d"
                % (
                    self.maxsize,
                    self.checkedin,
                    self.checkedout,
                )
        )
