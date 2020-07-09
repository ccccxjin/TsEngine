import pytest

from ..engine import TsEngine
from ..orm import Session
from ..pool import QueuePool

DB_INFO = dict(
    host='192.168.0.168',
    user='root',
    password='123456',
    database='test2'
)


@pytest.fixture(scope='session', autouse=True)
def engine():
    _engine = TsEngine(
        **DB_INFO,
        maxsize=3,
        wait=True,
        insert_chunk=None
    )
    yield _engine
    _engine.detach()


@pytest.fixture
def pool():
    _pool = QueuePool(
        maxsize=3,
        wait=True,
        db_info=DB_INFO
    )
    yield _pool
    _pool.detach()
    assert _pool.checkedin == 0
    assert _pool.checkedout == 0


@pytest.fixture
def session(engine):
    _session = Session(engine)
    yield _session
    _session.close()
