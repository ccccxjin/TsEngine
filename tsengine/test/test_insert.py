import functools
from datetime import datetime

import pytest


def prepare(func):
    @functools.wraps(func)
    def _func(self, session):
        try:
            sql = 'create table if not exists _test_insert_table (ts timestamp, name binary(20), age int, status bool, point float)'
            sql1 = """create table if not exists _test_insert_stable (ts timestamp, name binary(20), age int, status bool, point float) tags(sign int)"""
            session._execute(sql)
            session._execute(sql1)
            func(self, session)
        finally:
            session._execute('drop table if exists _test_insert_table')
            session._execute('drop table if exists _test_insert_stable')

    return _func


class TestInsert:

    @pytest.mark.skip
    @prepare
    def test_insert1(self, session):
        data = dict(
            ts=datetime(2020, 1, 1, 15, 0),
            name='cheney',
            age=20,
            status=True,
            point=1.1
        )
        session.insert('_test_insert_table').value(data).commit()
        res = session.query('_test_insert_table').dict()
        res[0]['point'] = round(res[0]['point'], 2)
        assert res[0] == data

    @pytest.mark.skip
    @prepare
    def test_insert2(self, session):
        data = dict(
            ts=datetime(2020, 1, 1, 15, 0),
            name='cheney',
            age=20,
            status=True
        )
        session.insert('_test_insert_table').columns('ts, name, age, status').value(data).commit()
        res = session.query('_test_insert_table').dict()
        data['point'] = None
        assert res[0] == data

    @pytest.mark.skip
    @prepare
    def test_insert3(self, session):
        data = [
            dict(
                ts=datetime(2020, 1, 1, 15, 0),
                name='cheney',
                age=20,
                status=True,
                point=1.1
            ),
            dict(
                ts=datetime(2020, 1, 2, 15, 0),
                name='cheney',
                age=20,
                status=False,
                point=1.1
            )
        ]
        session.insert('_test_insert_table').values(data).commit()
        res = session.query('_test_insert_table').dict()
        for o in res:
            o['point'] = round(o['point'], 2)
        assert res == data

    @pytest.mark.skip
    @prepare
    def test_insert4(self, session):
        data = dict(
            ts=datetime(2020, 1, 1, 15, 0),
            name='cheney',
            age=20,
            status=False,
            point=1.1
        )
        session.insert('_test_insert_table_1').using('_test_insert_stable').tags(sign=1).value(data).commit()
        data['sign'] = 1
        res = session.query('_test_insert_stable').dict()
        res[0]['point'] = round(res[0]['point'], 2)
        assert res[0] == data

    @pytest.mark.skip
    @prepare
    def test_insert5(self, session):
        data = [
            dict(
                ts=datetime(2020, 1, 1, 15, 0),
                name='cheney',
                age=20,
                status=True,
                point=1.1
            ),
            dict(
                ts=datetime(2020, 1, 2, 15, 0),
                name='cheney',
                age=20,
                status=False,
                point=1.1
            )
        ]
        session.insert('_test_insert_table_1').using('_test_insert_stable').tags(sign=1).values(data).commit()
        for o in data:
            o['sign'] = 1
        res = session.query('_test_insert_stable').dict()
        for o in res:
            o['point'] = round(o['point'], 2)
        assert res == data

    @pytest.mark.skip
    @prepare
    def test_insert5(self, session):
        data = [
            dict(
                ts=datetime(2020, 1, 1, 15, 0),
                name='cheney',
                age=20,
                status=True
            ),
            dict(
                ts=datetime(2020, 1, 2, 15, 0),
                name='cheney',
                age=20,
                status=False
            )
        ]
        session.insert('_test_insert_table_1').columns('ts, name, age, status').using('_test_insert_stable').tags(sign=1).values(data).commit()
        for o in data:
            o['sign'] = 1
            o['point'] = None
        res = session.query('_test_insert_stable').dict()
        assert res == data
