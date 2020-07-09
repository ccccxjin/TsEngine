import functools
from datetime import datetime, date

from .. import or_, and_
from ..util import f


def prepare(func):
    @functools.wraps(func)
    def _func(self, session):
        try:
            sql = """create table if not exists _test_query_table (ts timestamp, name binary(20), age int) tags(sign int)"""
            session._execute(sql)
            sql1 = """insert into _test_query_table_1 using _test_query_table tags(1) values('2020-01-01 15:00:00', 'cheney', 20)"""
            sql2 = """insert into _test_query_table_1 using _test_query_table tags(1) values('2020-01-03 15:00:00', 'summer', 18)"""
            sql3 = """insert into _test_query_table_2 using _test_query_table tags(2) values('2020-01-05 15:00:00', 'cheney', 20)"""
            sql4 = """insert into _test_query_table_2 using _test_query_table tags(2) values('2020-01-07 15:00:00', 'summer', 18)"""
            session._execute(sql1)
            session._execute(sql2)
            session._execute(sql3)
            session._execute(sql4)
            func(self, session)
        finally:
            session._execute('drop table if exists _test_query_table_1')
            session._execute('drop table if exists _test_query_table')

    return _func


class TestQuery:

    def query1(self, session):
        res = session.query('_test_query_table').all()
        except_res = [
            (datetime(2020, 1, 1, 15, 0), 'cheney', 20, 1),
            (datetime(2020, 1, 3, 15, 0), 'summer', 18, 1),
            (datetime(2020, 1, 5, 15, 0), 'cheney', 20, 2),
            (datetime(2020, 1, 7, 15, 0), 'summer', 18, 2),
        ]
        assert res == except_res

    def query2(self, session):
        res = session.query('_test_query_table').select('ts', 'sign').all()
        except_res = [
            (datetime(2020, 1, 1, 15, 0), 1),
            (datetime(2020, 1, 3, 15, 0), 1),
            (datetime(2020, 1, 5, 15, 0), 2),
            (datetime(2020, 1, 7, 15, 0), 2)
        ]
        assert res == except_res

    def query3(self, session):
        res = session.query('_test_query_table').select('ts', 'sign').filter(
            and_(
                or_(
                    f('name') == 'cheney',
                    f('name') == 'summer'
                ),
                f('ts') < date(2020, 1, 6)
            )
        ).all()
        except_res = [
            (datetime(2020, 1, 1, 15, 0), 1),
            (datetime(2020, 1, 3, 15, 0), 1),
            (datetime(2020, 1, 5, 15, 0), 2)
        ]
        assert res == except_res

    def query4(self, session):
        res = session.query('_test_query_table').select('sum(age)').filter(
            f('ts') > date(2020, 1, 2),
            f('ts') < date(2020, 1, 6)
        ).interval(d=1).fill(value=0).all()

        except_res = [
            (datetime(2020, 1, 2, 0, 0), 0),
            (datetime(2020, 1, 3, 0, 0), 18),
            (datetime(2020, 1, 4, 0, 0), 0),
            (datetime(2020, 1, 5, 0, 0), 20)
        ]
        assert res == except_res

    def query5(self, session):
        res = session.query('_test_query_table').select('sum(age)').group_by('sign').all()
        except_res = [(38, 1), (38, 2)]
        assert res == except_res

    def query6(self, session):
        res = session.query('_test_query_table').select('sign, sum(age)').filter(
            f('ts') > date(2020, 1, 2),
            f('ts') < date(2020, 1, 6)
        ).interval(d=1).fill(value=0).group_by('sign').all()
        except_res = [
            (datetime(2020, 1, 2, 0, 0), 0, 0, 1),
            (datetime(2020, 1, 3, 0, 0), 1, 18, 1),
            (datetime(2020, 1, 4, 0, 0), 0, 0, 1),
            (datetime(2020, 1, 5, 0, 0), 0, 0, 1),
            (datetime(2020, 1, 2, 0, 0), 0, 0, 2),
            (datetime(2020, 1, 3, 0, 0), 0, 0, 2),
            (datetime(2020, 1, 4, 0, 0), 0, 0, 2),
            (datetime(2020, 1, 5, 0, 0), 2, 20, 2)
        ]
        assert res == except_res

    def query7(self, session):
        res = session.query('_test_query_table').select('sign, sum(age)').filter(
            f('ts') > date(2020, 1, 2),
            f('ts') < date(2020, 1, 6)
        ).interval(d=1).fill(value=0).group_by('sign').dict()
        except_res = [
            {'ts': datetime(2020, 1, 2, 0, 0), 'sign': 1, 'sum(age)': 0},
            {'ts': datetime(2020, 1, 3, 0, 0), 'sign': 1, 'sum(age)': 18},
            {'ts': datetime(2020, 1, 4, 0, 0), 'sign': 1, 'sum(age)': 0},
            {'ts': datetime(2020, 1, 5, 0, 0), 'sign': 1, 'sum(age)': 0},
            {'ts': datetime(2020, 1, 2, 0, 0), 'sign': 2, 'sum(age)': 0},
            {'ts': datetime(2020, 1, 3, 0, 0), 'sign': 2, 'sum(age)': 0},
            {'ts': datetime(2020, 1, 4, 0, 0), 'sign': 2, 'sum(age)': 0},
            {'ts': datetime(2020, 1, 5, 0, 0), 'sign': 2, 'sum(age)': 20}
        ]
        assert res == except_res

    @prepare
    def test_query(self, session):
        self.query1(session)
        self.query2(session)
        self.query3(session)
        self.query4(session)
        self.query5(session)
        self.query6(session)
        self.query7(session)
