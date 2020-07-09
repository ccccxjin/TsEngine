import numbers
from datetime import datetime, date

from ..constants import FieldDescribe

__all__ = [
    'Field',
    'TimeStamp',
    'Int', 'BigInt', 'SmallInt', 'TinyInt',
    'Float', 'Double',
    'Binary', 'NChar',
    'Bool'
]


class Field:
    def __init__(self, name=''):
        self._value = None
        self.name = name

    def __get__(self, instance, owner):
        return self._value

    @property
    def _create_sql(self):
        return '%s %s' % (self.name, self._field_type)


class TimeStamp(Field):
    _field_type = FieldDescribe.TimeStamp
    _field_length = 8
    _min = datetime(1970, 1, 1)

    def __init__(self, name):
        super().__init__(name)

    def __set__(self, instance, value):
        if isinstance(value, datetime):
            self._check(value)
            self._value = value
            return
        if isinstance(value, date):
            value = datetime(value.year, value.month, value.day)
            self._check(value)
            self._value = value
            return
        if isinstance(value, str):
            try:
                value = datetime.strptime(value, '%Y-%m-%d')
                self._check(value)
                self._value = value
            except ValueError:
                try:
                    value = datetime.strptime(value, '%Y-%m-%d %H-%M-%S')
                    self._check(value)
                    self._value = value
                except ValueError:
                    raise ValueError('%s format error' % self.name)
            return

        if isinstance(value, (int, float)):
            value = datetime.fromtimestamp(value)
            self._check(value)
            self._value = value
            return
        raise ValueError('%s invalid')

    def _check(self, value):
        if value < self._min:
            raise ValueError('%s cannot be earlier than "1970-01-01"' % self.name)


class _Integer(Field):

    def __init_subclass__(cls, **kwargs):
        if 'minimum' not in cls.__dict__ or 'maximum' not in cls.__dict__:
            raise NotImplementedError("Cannot inherit class _Integer without minimum or maximum attribute")

    def __init__(self, name):
        super().__init__(name)
        if not isinstance(self.minimum, numbers.Integral):
            raise ValueError('minimum must be integer')
        if not isinstance(self.maximum, numbers.Integral):
            raise ValueError('maximum must be integer')
        if self.minimum > self.maximum:
            raise ValueError('minimum can not be greater than maximum')

    def __set__(self, instance, value):
        if not isinstance(value, numbers.Integral):
            raise ValueError('%s must be integer' % self.name)
        if not self.minimum <= value <= self.maximum:
            raise ValueError('%s must be between minimum and maximum' % self.name)
        self._value = value


class Int(_Integer):
    _field_type = FieldDescribe.Int
    _field_length = 4

    minimum = pow(-2, 31) + 1
    maximum = pow(2, 31) - 1


class BigInt(_Integer):
    _field_type = FieldDescribe.BigInt
    _field_length = 8

    minimum = pow(-2, 63) + 1
    maximum = pow(2, 63) - 1


class SmallInt(_Integer):
    _field_type = FieldDescribe.SmallInt
    _field_length = 2

    minimum = -32767
    maximum = 32767


class TinyInt(_Integer):
    _field_type = FieldDescribe.TinyInt
    _field_length = 1

    minimum = -127
    maximum = 127


class _Float(Field):

    def __init_subclass__(cls, **kwargs):
        if 'minimum' not in cls.__dict__ or 'maximum' not in cls.__dict__:
            raise NotImplementedError("Cannot inherit class _Integer without minimum or maximum attribute")

    def __init__(self, name):
        super().__init__(name)
        if not isinstance(self.minimum, float):
            raise ValueError('minimum must be float')
        if not isinstance(self.maximum, float):
            raise ValueError('maximum must be float')
        if self.minimum > self.maximum:
            raise ValueError('minimum can not be greater than maximum')

    def __set__(self, instance, value):
        if not isinstance(value, float):
            raise ValueError('%s must be float' % self.name)
        if not self.minimum <= value <= self.maximum:
            raise ValueError('%s must be between minimum and maximum' % self.name)
        self._value = value


class Float(_Float):
    _field_type = FieldDescribe.Float
    _field_length = 4

    minimum = 3.4E38
    maximum = 3.4E38


class Double(_Float):
    _field_type = FieldDescribe.Double
    _field_length = 8

    minimum = 1.7E308
    maximum = 1.7E308


class _String(Field):
    _field_length = None

    def __init__(self, name, length):
        super().__init__(name)
        self.length = self._field_length = length

    def __set__(self, instance, value):
        if not isinstance(value, str):
            raise ValueError('%s must be string' % self.name)
        self._value = value

    @property
    def _create_sql(self):
        return '%s %s(%s)' % (self.name, self._field_type, self.length)


class Binary(_String):
    _field_type = FieldDescribe.Binary
    _max_length = 65526

    def __init__(self, name, length):
        super().__init__(name, length)

    def __set__(self, instance, value):
        if len(value) > self.length:
            raise ValueError('%s length must less than %s' % (self.name, self._max_length))
        super().__set__(instance, value)


class NChar(_String):
    _field_type = FieldDescribe.NChar

    def __init__(self, name, length):
        super().__init__(name, length)


class Bool(Field):
    _field_type = FieldDescribe.Bool
    _field_length = 1

    def __init__(self, name):
        super().__init__(name)

    def __set__(self, instance, value):
        self._value = True if value else False
