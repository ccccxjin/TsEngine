from .operation import Operation
from ..util._collections import _escape_column, _escape_mapper_args


class Insert(Operation):
    def insert(self, table):
        """
        examples:
            insert('table1')
        """
        if not table:
            raise ValueError('table cannot be empty')
        self._table = table
        sql = 'insert into %s' % table
        self._sql.append(sql)
        return self

    def columns(self, *args):
        """
        examples:
            columns('ia')
            columns('ia', 'ib')
            columns('ia, ib')
            columns(['ia', 'ib'])
        """
        if not args:
            raise ValueError('columns cannot be empty')
        args = _escape_column(args)
        self._col_lst = args
        sql = args.__str__()
        self._sql.append(sql)
        return self

    def using(self, stable):
        """
        examples:
            using('table1')
        """
        if not stable:
            raise ValueError('stable cannot be empty')
        self._stable = stable
        sql = 'using %s' % stable
        self._sql.append(sql)
        return self

    def tags(self, **kwargs):
        """
        examples:
            tags(id=1, name='cheney')
        """
        if not kwargs:
            raise ValueError('tags cannot be empty')

        kwargs = _escape_mapper_args(kwargs)
        _tag_lst = self._determine_tags(self._stable)

        if set(_tag_lst) != set(kwargs):
            raise ValueError('tags and parameters do not match')

        sql = 'tags' + self.template(_tag_lst).format(**kwargs)
        self._sql.append(sql)
        return self

    def value(self, arg):
        """
        examples:
            value(dict(id=1, name='cheney', age=20))
        """
        sql = ' values ' + self._value(arg)
        self._sql.append(sql)
        self.session.insert_buffer.append(str(self))
        return self

    def values(self, args):
        """
        examples:
            value(
                [
                    dict(id=1, name='cheney', age=20),
                    dict(id=2, name='summer', age=20),
                    dict(id=3, name='jerry', age=20)
                ]
            )
        """
        if not args:
            raise ValueError('Value cannot be empty')
        _chunk_size = self.session.chunk_size
        if not _chunk_size:
            sql = ' values ' + ' '.join([self._value(arg) for arg in args])
            self._sql.append(str(sql))
            self.session.insert_buffer.append(str(self))
        else:
            _sql = self._sql.copy()
            self._sql = []
            for o in range(0, len(args), _chunk_size):
                _sql_copy = _sql.copy()
                arg = args[o: o + _chunk_size]
                sql = ' values ' + ' '.join([self._value(o) for o in arg])
                _sql_copy.append(str(sql))
                _sql_copy = ' '.join(_sql_copy)
                self._sql.append(_sql_copy)
                self.session.insert_buffer.append(_sql_copy)
        return self

    def _value(self, arg):
        if not arg:
            raise ValueError('Value cannot be empty')

        if not isinstance(arg, dict):
            raise ValueError('Invalid value')

        arg = _escape_mapper_args(arg)
        if getattr(self, '_stable', None):
            _col_lst = self._determine_cols(self._stable)
        else:
            _col_lst = self._determine_cols(self._table)

        if set(_col_lst) != set(arg):
            raise ValueError('fields and parameters do not match')

        sql = self.template(_col_lst).format(**arg)
        return sql

    def commit(self):
        return self.session.commit()
