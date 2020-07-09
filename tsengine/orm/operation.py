class Operation:
    __slots__ = ['session', '_sql', '_table', '_stable', '_col_lst', '_tag_lst', '_operation_history']

    def __init__(self, session):
        self.session = session
        self._sql = []
        self._table = None
        self._stable = None
        self._col_lst = None
        self._tag_lst = None
        self._operation_history = dict()

    def _determine_cols(self, table):
        _col_lst = self._col_lst
        _all_cols = self.session.get_columns_cache(table)

        if not _col_lst:
            return _all_cols

        if not (set(_col_lst) <= set(_all_cols)):
            raise ValueError('Invalid column')

        return [o for o in _all_cols if o in _col_lst]

    def _determine_tags(self, table):
        _tag_lst = self._tag_lst
        _all_tags = self.session.get_tags_cache(table)

        if not _tag_lst:
            return _all_tags

        if not (set(_tag_lst) < set(_all_tags)):
            raise ValueError('Invalid tag')

        return [o for o in _all_tags if o in _tag_lst]

    def __repr__(self):
        return ' '.join(self._sql)

    @staticmethod
    def template(args):
        """
        according to the order of the fields, return string template
        example:
            args: (col1, col2, ...)
        return:
            '({col1}, {col2}, ...)'
        """
        args = [o.strip(' ') for o in args]
        return '(' + ', '.join(['{%s}' % o for o in args]) + ')'
