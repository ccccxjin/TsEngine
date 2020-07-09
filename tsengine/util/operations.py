from ._collections import _escape_mapper_args


class F:
    __slots__ = 'name'

    def __init__(self, name):
        self.name = name

    def __lt__(self, other):
        other = _escape_mapper_args(other)
        return '%s < %s' % (self.name, other)

    def __le__(self, other):
        other = _escape_mapper_args(other)
        return '%s <= %s' % (self.name, other)

    def __gt__(self, other):
        other = _escape_mapper_args(other)
        return '%s > %s' % (self.name, other)

    def __ge__(self, other):
        other = _escape_mapper_args(other)
        return '%s >= %s' % (self.name, other)

    def __eq__(self, other):
        other = _escape_mapper_args(other)
        return '%s = %s' % (self.name, other)

    def __ne__(self, other):
        other = _escape_mapper_args(other)
        return '%s != %s' % (self.name, other)

    def like(self, other):
        other = _escape_mapper_args(other)
        return "%s like '%s' " % (self.name, other)


f = F


def or_(*args):
    return '(' + ' or '.join(args) + ')'


def and_(*args):
    return ' and '.join(args)
