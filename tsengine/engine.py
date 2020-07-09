from .pool import QueuePool


class TsEngine:
    def __init__(self, host, user, password, database=None, port=6030, config=None, maxsize=5, wait=True, insert_chunk=None):
        _db_info = dict(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            config=config
        )
        self.pool = QueuePool(maxsize, wait, _db_info)

        self.maxsize = maxsize
        self.wait = wait
        self.insert_chunk = insert_chunk

        self.columns_cache = dict()
        self.tags_cache = dict()

    def detach(self):
        self.pool.detach()
