## TsEngine

This is the function package of Taos database.

### Introduction

Major TsEngine features include:
- Creating data tables using Python class objects
- Implement connection pool
- Using Python and SQL statements to operate the database

### Installing

`$ pip install tsengine`

### Example

create engine
```python
from tsengine import TsEngine

engine = TsEngine(
        host='127.0.0.1',
        user='root',
        password='',
        database='test_db'
    )
```

create session
```python

from tsengine import SessionFactory
Session = SessionFactory(engine)
```

create scope_session
```python
from tsengine import ScopeSessionFactory
Session = ScopeSessionFactory(engine)
```

create table
```python
from tsengine import Model, TimeStamp, Int, Binary

class Info(Model):
    ts = TimeStamp('ts')
    name = Binary('name', length=20)
    age = Int('age')

    class Meta:
        table_type = 'table'
        table_name = 'info'

Model.create_all(session)
```

create stable
```python
from tsengine import Model, TimeStamp, Int, Binary

class SInfo(Model):
        ts = TimeStamp('ts')
        sign = Int('sign')
        name = Binary('name', length=20)
        age = Int('age')
    
        class Meta:
            table_type = 'stable'
            table_name = 'sinfo'
            tags_name = 'sign, name'
    
Model.create_all(session)
```

create sub table
```python
from tsengine import Model

class SubInfo(Model):
    class Meta:
        table_type = 'sub_table'
        stable_name = 'sinfo'
        table_name = 'sub_info_1_cheney'
        tags_value = {'sign': 1, 'name': 'cheney'}
```

Query
```python
from tsengine import f

session.query('sinfo').select('ts, name, age').filter(f('name') == 'cheney').all()
session.query('sinfo').select('ts, name, age').filter(f('name') == 'cheney').dict()
```

Insert
```python
from datetime import date

data = dict(ts=date(2019, 10, 1), name='cheney', age=20, sign=1)
session.insert('sinfo').value(data)

data = dict(ts=date(2019, 10, 2), name='cheney')
session.insert('sinfo').column('ts, name').value(data)

data = [
    dict(ts=date(2019, 10, 3), age=20),
    dict(ts=date(2019, 10, 4), age=20),
    dict(ts=date(2019, 10, 5), age=20)
]
session.insert('sub_table').using('sinfo').tags(sign=1, name='cheney').values(data)
```
