import functools

from .session import Session
from ..util._collections import ThreadLocalRegistry


class SessionFactory:
    def __init__(self, engine=None, creator=Session, chunk_size=None):
        self.engine = engine
        self.creator = creator
        self.chunk_size = chunk_size

    def __call__(self, engine=None, chunk_size=None):
        _engine = engine if engine else self.engine
        _chunk_size = chunk_size if chunk_size else self.chunk_size
        return Session(_engine, _chunk_size)


class ScopeSessionFactory:
    """
    thread safe session
    """
    def __init__(self, engine=None, creator=Session, chunk_size=None):
        self.engine = engine
        self.creator = creator
        self.chunk_size = chunk_size
        if engine is not None:
            _creator = functools.partial(self.creator, engine, self.chunk_size)
            self.registry = ThreadLocalRegistry(_creator)
        else:
            self.registry = ThreadLocalRegistry(self.creator)

    def __call__(self, engine=None, chunk_size=None):
        _engine = engine if engine else self.engine
        _chunk_size = chunk_size if chunk_size else self.chunk_size
        if self.registry.has():
            return self.registry()
        else:
            session = self.creator(_engine, _chunk_size)
            self.registry.set(session)
            return session
