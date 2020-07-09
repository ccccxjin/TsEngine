from .operation import Operation
from ..util._collections import _escape_column, _escape_mapper_args


class Query(Operation):
    def query(self, table):
        """
        examples:
            query('table1')
        """
        if not table:
            raise ValueError('table cannot be empty')

        self._table = table
        sql = 'select * from %s' % table
        self._sql.append(sql)
        return self

    def select(self, *args):
        """
        args: columns
        examples:
            select('ia')
            select('ia', 'ib')
            select('ia, ib')
            select(['ia', 'ib'])
        """
        if not args:
            raise ValueError('select cannot be empty')
        args = _escape_column(args)
        self._operation_history['select'] = args
        sql = self._sql.pop()
        sql = sql.replace('*', ', '.join(args))
        self._sql.append(sql)
        return self

    def filter(self, *args):
        """
        kwargs: columns values
        examples:
            filter(
                f(ts) > date(2019, 10, 1),
                f(name) == 'cheney'
            )
        """
        if not args:
            raise ValueError('Filter cannot be empty')
        sql = ' where ' + ' and '.join(args)
        self._sql.append(sql)
        return self

    def interval(self, **kwargs):
        """
        interval(d=1)
        """
        if not kwargs:
            raise ValueError('interval cannot be empty')
        if len(kwargs) > 1:
            raise ValueError('Interval has at most one parameter')
        self._operation_history['interval'] = True
        _unit, _several = kwargs.popitem()
        sql = ' interval(%s%s) ' % (_several, _unit)
        self._sql.append(sql)
        return self

    def fill(self, *args, **kwargs):
        """
        examples:
            fill('null')
            fill(value=1)
        """
        _length = len(args) + len(kwargs)
        if not _length:
            raise ValueError('fill cannot be empty')
        if _length > 1:
            raise ValueError('fill has at most one parameter')
        if args:
            sql = ' fill(%s) ' % args[0]
        else:
            kwargs = _escape_mapper_args(kwargs)
            sql = ' fill(%s, %s) ' % kwargs.popitem()
        self._sql.append(sql)
        return self

    def sliding(self):
        raise NotImplementedError('method(sliding) is invalid')

    def group_by(self, *args):
        """
        examples:
            group_by('id')
            group_by('id', 'name')
            group_by('id, name')
            group_by(['id', 'name'])
        """
        if not args:
            raise ValueError('group_by cannot be empty')
        args = _escape_column(args)
        self._operation_history['group_by'] = args
        sql = ' group by ' + ', '.join(args)
        self._sql.append(sql)
        return self

    def order_by(self, *args, desc=False):
        """
        examples:
            order_by('id')
            order_by('id', 'name')
            order_by('id, name')
            order_by(['id', 'name'])
        """
        if not args:
            raise ValueError('order_by cannot be empty')
        args = _escape_column(args)
        sql = ' order by ' + ', '.join(args)
        if desc:
            sql = sql + ' desc '
        self._sql.append(sql)
        return self

    def having(self):
        raise NotImplementedError('method(having) is invalid')

    def slimit(self, slimit, soffset=None):
        sql = ' slimit %s ' % slimit
        if soffset:
            sql += ' soffset(%s) ' % soffset
        self._sql.append(sql)
        return self

    def limit(self, limit, offset=None):
        sql = ' limit %s ' % limit
        if offset:
            sql += ' offset(%s) ' % offset
        self._sql.append(sql)
        return self

    def all(self):
        return self.session.execute(str(self))

    def dict(self):
        """
        return dictionary result
        """
        res = self.session.execute(str(self))
        keys = list()
        if 'interval' in self._operation_history:
            keys.append('ts')
        if 'select' in self._operation_history:
            keys.extend(self._operation_history['select'])
        else:
            _cols_lst = self.session.get_columns_cache(self._table)
            keys.extend(_cols_lst)
            _tags_lst = self.session.get_tags_cache(self._table)
            keys.extend(_tags_lst)
        if 'group_by' in self._operation_history:
            keys.extend(self._operation_history['group_by'])
        return [dict(zip(keys, o)) for o in res]
