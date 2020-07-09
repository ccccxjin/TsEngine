import threading

from ..orm.session_factory import ScopeSessionFactory


class TestSession:

    def func(self, Session):
        s1 = Session()
        s2 = Session()
        assert s1 is s2

    def test_scope_session(self, engine):
        Session = ScopeSessionFactory(engine)
        ts = [threading.Thread(target=self.func, args=(Session,)) for _ in range(10)]
        [t.start() for t in ts]
        [t.join() for t in ts]
