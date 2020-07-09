import pytest

from ..mapping import *


@pytest.mark.skip
def test_create_table(session):
    class Info(Model):
        ts_field = TimeStamp('ts_field')
        int_field = Int('int_field')
        bigint_field = BigInt('bigint_field')
        smallint_field = SmallInt('smallint_field')
        tiny_field = TinyInt('tiny_field')
        float_field = Float('float_field')
        double_field = Double('double_field')
        binary_field = Binary('binary_field', length=20)
        nchar_field = NChar('nchar_field', length=30)
        bool_field = Bool('bool_field')

        class Meta:
            table_type = 'table'
            table_name = 'info'

    except_desc = [
        ('ts_field', 'TIMESTAMP', 8, ''),
        ('int_field', 'INT', 4, ''),
        ('bigint_field', 'BIGINT', 8, ''),
        ('smallint_field', 'SMALLINT', 2, ''),
        ('tiny_field', 'TINYINT', 1, ''),
        ('float_field', 'FLOAT', 4, ''),
        ('double_field', 'DOUBLE', 8, ''),
        ('binary_field', 'BINARY', 20, ''),
        ('nchar_field', 'NCHAR', 30, ''),
        ('bool_field', 'BOOL', 1, '')
    ]
    try:
        Model.create_all(session)

        info_desc = session.execute('describe info')
        assert info_desc == except_desc
    finally:
        session.execute('drop table if exists info')


@pytest.mark.skip
def test_create_stable(session):
    class SInfo(Model):
        ts_field = TimeStamp('ts_field')
        int_field = Int('int_field')
        bigint_field = BigInt('bigint_field')
        smallint_field = SmallInt('smallint_field')
        tiny_field = TinyInt('tiny_field')
        float_field = Float('float_field')
        double_field = Double('double_field')
        binary_field = Binary('binary_field', length=20)
        nchar_field = NChar('nchar_field', length=30)
        bool_field = Bool('bool_field')

        class Meta:
            table_type = 'stable'
            stable_name = 'sinfo'
            tags_name = 'int_field, float_field, binary_field'

    except_desc = [
        ('ts_field', 'TIMESTAMP', 8, ''),
        ('bigint_field', 'BIGINT', 8, ''),
        ('smallint_field', 'SMALLINT', 2, ''),
        ('tiny_field', 'TINYINT', 1, ''),
        ('double_field', 'DOUBLE', 8, ''),
        ('nchar_field', 'NCHAR', 30, ''),
        ('bool_field', 'BOOL', 1, ''),
        ('int_field', 'INT', 4, 'tag'),
        ('float_field', 'FLOAT', 4, 'tag'),
        ('binary_field', 'BINARY', 20, 'tag')
    ]
    try:
        Model.create_all(session)

        sinfo_desc = session.execute('describe sinfo')
        assert sinfo_desc == except_desc
    finally:
        session.execute('drop table if exists sinfo')


@pytest.mark.skip
def test_create_subtable(session):
    class SInfo1(Model):
        ts_field = TimeStamp('ts_field')
        int_field = Int('int_field')
        bigint_field = BigInt('bigint_field')
        smallint_field = SmallInt('smallint_field')
        tiny_field = TinyInt('tiny_field')
        float_field = Float('float_field')
        double_field = Double('double_field')
        binary_field = Binary('binary_field', length=20)
        nchar_field = NChar('nchar_field', length=30)
        bool_field = Bool('bool_field')

        class Meta:
            table_type = 'stable'
            stable_name = 'sinfo1'
            tags_name = 'int_field, float_field, binary_field'

    class SubInfo(Model):
        class Meta:
            table_type = 'sub_table'
            stable_name = 'sinfo1'
            table_name = 'sub_info'
            tags_value = {'int_field': 1, 'float_field': 1.2, 'binary_field': 'sub_info_112'}

    except_desc = [
        ('ts_field', 'TIMESTAMP', 8, ''),
        ('bigint_field', 'BIGINT', 8, ''),
        ('smallint_field', 'SMALLINT', 2, ''),
        ('tiny_field', 'TINYINT', 1, ''),
        ('double_field', 'DOUBLE', 8, ''),
        ('nchar_field', 'NCHAR', 30, ''),
        ('bool_field', 'BOOL', 1, ''),
        ('int_field', 'INT', 4, '1'),
        ('float_field', 'FLOAT', 4, '1.200000'),
        ('binary_field', 'BINARY', 20, 'sub_info_112')
    ]
    try:
        Model.create_all(session)
        sub_info_desc = session.execute('describe sub_info')
        assert sub_info_desc == except_desc
    finally:
        session.execute('drop table if exists sub_info')
        session.execute('drop table if exists sinfo1')
