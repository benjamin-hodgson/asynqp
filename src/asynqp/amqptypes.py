import abc
import datetime
import functools
import operator
from . import serialisation


MAX_OCTET = 0xFF
MAX_SHORT = 0xFFFF
MAX_LONG = 0xFFFFFFFF
MAX_LONG_LONG = 0xFFFFFFFFFFFFFFFF


class FieldType(abc.ABC):
    def __init__(self, value):
        if isinstance(value, type(self)):
            value = value.value  # ha!
        if not self.isvalid(value):
            raise TypeError('{} is not a valid value for type {}'.format(value, type(self).__name__))
        self.value = value

    def __eq__(self, other):
        if isinstance(other, type(self.value)):
            return self.value == other
        try:
            return self.value == other.value
        except AttributeError:
            return NotImplemented

    def __repr__(self):
        return "{}({})".format(type(self).__name__, self.value)

    @classmethod
    @abc.abstractmethod
    def isvalid(cls, value):
        pass

    @abc.abstractmethod
    def write(self, stream):
        pass

    @classmethod
    @abc.abstractmethod
    def read(cls, stream):
        pass

    def operate(self, func, other):
        if isinstance(other, type(self.value)):
            return type(self)(func(self.value, other))
        try:
            return type(self)(func(self.value, other.value))
        except (AttributeError, TypeError):
            return NotImplemented


class HashableFieldType(FieldType):
    def __hash__(self):
        return hash(self.value)


@functools.total_ordering
class OrderedFieldType(FieldType):
    __lt__ = functools.partialmethod(FieldType.operate, operator.lt)


class NumericFieldType(OrderedFieldType, HashableFieldType):
    __add__ = functools.partialmethod(FieldType.operate, operator.add)
    __sub__ = functools.partialmethod(FieldType.operate, operator.sub)
    __mul__ = functools.partialmethod(FieldType.operate, operator.mul)
    __truediv__ = functools.partialmethod(FieldType.operate, operator.truediv)
    __floordiv__ = functools.partialmethod(FieldType.operate, operator.floordiv)


class IntegralFieldType(NumericFieldType):
    def __int__(self):
        return int(self.value)

    def __index__(self):
        return operator.index(self.value)


class StringFieldType(HashableFieldType):
    def __str__(self):
        return str(self.value)


class Bit(IntegralFieldType):
    @classmethod
    def isvalid(cls, value):
        return isinstance(value, bool)

    def write(self, stream):
        raise NotImplementedError

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_bool(stream))


class Octet(IntegralFieldType):
    @classmethod
    def isvalid(cls, value):
        return isinstance(value, int) and 0 <= value <= MAX_OCTET

    def write(self, stream):
        stream.write(serialisation.pack_octet(self.value))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_octet(stream))


class Short(IntegralFieldType):
    @classmethod
    def isvalid(cls, value):
        return isinstance(value, int) and 0 <= value <= MAX_SHORT

    def write(self, stream):
        stream.write(serialisation.pack_short(self.value))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_short(stream))


class Long(IntegralFieldType):
    @classmethod
    def isvalid(cls, value):
        return isinstance(value, int) and 0 <= value <= MAX_LONG

    def write(self, stream):
        stream.write(serialisation.pack_long(self.value))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_long(stream))


class LongLong(IntegralFieldType):
    @classmethod
    def isvalid(cls, value):
        return isinstance(value, int) and 0 <= value <= MAX_LONG_LONG

    def write(self, stream):
        stream.write(serialisation.pack_long_long(self.value))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_long_long(stream))


class ShortStr(StringFieldType):
    @classmethod
    def isvalid(cls, value):
        return isinstance(value, str) and len(value) <= MAX_OCTET

    def write(self, stream):
        stream.write(serialisation.pack_short_string(self.value))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_short_string(stream))


class LongStr(StringFieldType):
    @classmethod
    def isvalid(cls, value):
        return isinstance(value, str) and len(value) <= MAX_LONG

    def write(self, stream):
        stream.write(serialisation.pack_long_string(self.value))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_long_string(stream))


class Table(FieldType):
    @classmethod
    def isvalid(cls, value):
        return isinstance(value, dict)

    def write(self, stream):
        stream.write(serialisation.pack_table(self.value))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_table(stream))


class Timestamp(FieldType):
    def __init__(self, value):
        if isinstance(value, int):
            value = datetime.datetime.fromtimestamp(value)
        super().__init__(value)

    def __eq__(self, other):
        if isinstance(other, type(self.value)):
            return abs(self.value - other) < datetime.timedelta(seconds=1)
        try:
            return abs(self.value - other.value) < datetime.timedelta(seconds=1)
        except (AttributeError, TypeError):
            return NotImplemented

    @classmethod
    def isvalid(cls, value):
        return isinstance(value, datetime.datetime)

    def write(self, stream):
        stamp = int(self.value.timestamp())
        stream.write(serialisation.pack_long_long(stamp))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_long_long(stream))


FIELD_TYPES = {
    'bit': Bit,
    'octet': Octet,
    'short': Short,
    'long': Long,
    'longlong': LongLong,
    'table': Table,
    'longstr': LongStr,
    'shortstr': ShortStr,
    'timestamp': Timestamp
}
