from taos.error import OperationalError

from .insert import Insert
from .query import Query
from ..exc import InvalidOperationError


class Session:
    def __init__(self, engine=None, chunk_size=None):
        self.engine = engine

        self.insert_buffer = list()
        self.available = True
        self.activation = False
        self.connection = None
        self.cursor = None
        self.database = None
        self.chunk_size = chunk_size

    def _init(self):
        if self.available:
            self.available = False
            if self.activation:
                self.engine.pool.put(self.connection)
                self.connection = None
                self.cursor.close()
                self.cursor = None
            self.database = None
            self.engine = None
            self.insert_buffer = list()

    def bind_db(self, database):
        self.database = database
        self._execute('use %s' % database)

    def bind_engine(self, engine):
        self._init()
        self.engine = engine

    def execute(self, statement):
        """
        if insert_buffer is True, the statements in the insert_buffer are executed first
        """
        if not self.available:
            raise InvalidOperationError('session is not available')
        if self.insert_buffer:
            self.commit()
        return self._execute(statement)

    def _execute(self, statement):
        """
        If don't want to execute the statement in the insert_buffer, use this method
        """
        if not self.available:
            raise InvalidOperationError('session is not available')

        if not self.activation:
            self.activate()

        count = self.cursor.execute(str(statement))
        try:
            return self.cursor.fetchall()
        except OperationalError:
            return count

    def activate(self):
        """
        activate the session and check the session
        major: get connection from pool and check whether the session has a database
        """
        if not self.available:
            raise InvalidOperationError('session is not available')
        if not self.activation:
            self.activation = True
            if not self.engine:
                raise ValueError('session must bind engine operation')
            self.connection = self.engine.pool.get()
            self.cursor = self.connection.cursor()
            self.database = self.connection._database
            if not self.database:
                raise ValueError('session must bind database before operation')

    def commit(self):
        count = 0
        for sql in self.insert_buffer:
            res = self._execute(sql)
            if res is not None:
                count += res
        return count

    def close(self):
        self._init()

    def get_columns_cache(self, table):
        """
        returns the field of the table
        """
        if not getattr(self, 'database'):
            self.database = self.engine.database
        columns = self.engine.columns_cache.get((self.database, table), None)
        if columns is None:
            table_desc = self._execute('describe %s' % table)
            columns = [o[0] for o in table_desc if o[3] != 'tag']
            self.engine.columns_cache[(self.database, table)] = columns
        return columns

    def get_tags_cache(self, table):
        """
        returns the tags of the table
        """
        if not getattr(self, 'database'):
            self.database = self.engine.database
        tags = self.engine.tags_cache.get((self.database, table), None)
        if tags is None:
            table_desc = self._execute('describe %s' % table)
            tags = [o[0] for o in table_desc if o[3] == 'tag']
            self.engine.tags_cache[(self.database, table)] = tags
        return tags

    def query(self, table):
        return Query(self).query(table)

    def insert(self, table):
        return Insert(self).insert(table)
