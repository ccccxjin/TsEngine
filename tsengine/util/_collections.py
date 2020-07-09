import threading
from datetime import datetime, date


def _escape_mapper_args(args):
    """
    transfer data
    """
    if isinstance(args, int):
        return args
    if isinstance(args, str):
        return '\'' + args + '\''
    if isinstance(args, bool):
        return 1 if args else 0
    if isinstance(args, (datetime, date)):
        return '\'' + args.strftime('%Y-%m-%d %H:%M:%S.%f') + '\''
    if isinstance(args, (tuple, list)):
        return [_escape_mapper_args(arg) for arg in args]
    if isinstance(args, dict):
        return {k: _escape_mapper_args(v) for k, v in args.items()}
    return args


def _escape_column(column):
    """
    transfer columns, return tuple, such as ('a', 'b', 'c'),
    when constructing SQL, direct  use the__ str__ () method
    """
    if not len(column):
        raise ValueError('Field cannot be empty')
    if len(column) == 1:
        column = column[0]
        if isinstance(column, str):
            column = tuple(o.strip(' ') for o in column.split(','))
            return column
        if isinstance(column, (tuple, list)):
            return tuple(o.strip(' ') for o in column)
        raise ValueError('column error')
    return column


class ThreadLocalRegistry:
    """
    copy from sqlalchemy...
    """
    def __init__(self, creator):
        self.creator = creator
        self.registry = threading.local()

    def __call__(self):
        try:
            return self.registry.value
        except AttributeError:
            val = self.registry.value = self.creator()
            return val

    def has(self):
        return hasattr(self.registry, "value")

    def set(self, obj):
        self.registry.value = obj

    def clear(self):
        try:
            del self.registry.value
        except AttributeError:
            pass
