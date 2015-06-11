import datetime
from . import serialisation


class Bit(object):
    def __init__(self, value):
        if isinstance(value, type(self)):
            value = value.value  # ha!
        if not isinstance(value, bool):
            raise TypeError('Could not construct a Bit from value {}'.format(value))
        self.value = value

    def __eq__(self, other):
        if isinstance(other, type(self.value)):
            return self.value == other
        try:
            return self.value == other.value
        except AttributeError:
            return NotImplemented

    def __bool__(self):
        return self.value

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_bool(stream))


class Octet(int):
    MIN = 0
    MAX = (1 << 8) - 1

    def __new__(cls, value):
        if not (Octet.MIN <= value <= Octet.MAX):
            raise TypeError('Could not construct an Octet from value {}'.format(value))
        return super().__new__(cls, value)

    def write(self, stream):
        stream.write(serialisation.pack_octet(self))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_octet(stream))


class Short(int):
    MIN = -(1 << 15)
    MAX = (1 << 15) - 1

    def __new__(cls, value):
        if not (Short.MIN <= value <= Short.MAX):
            raise TypeError('Could not construct a Short from value {}'.format(value))
        return super().__new__(cls, value)

    def write(self, stream):
        stream.write(serialisation.pack_short(self))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_short(stream))


class UnsignedShort(int):
    MIN = 0
    MAX = (1 << 16) - 1

    def __new__(cls, value):
        if not (UnsignedShort.MIN <= value <= UnsignedShort.MAX):
            raise TypeError('Could not construct an UnsignedShort from value {}'.format(value))
        return super().__new__(cls, value)

    def write(self, stream):
        stream.write(serialisation.pack_unsigned_short(self))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_unsigned_short(stream))


class Long(int):
    MIN = -(1 << 31)
    MAX = (1 << 31) - 1

    def __new__(cls, value):
        if not (Long.MIN <= value <= Long.MAX):
            raise TypeError('Could not construct a Long from value {}'.format(value))
        return super().__new__(cls, value)

    def write(self, stream):
        stream.write(serialisation.pack_long(self))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_long(stream))


class UnsignedLong(int):
    MIN = 0
    MAX = (1 << 32) - 1

    def __new__(cls, value):
        if not (UnsignedLong.MIN <= value <= UnsignedLong.MAX):
            raise TypeError('Could not construct a UnsignedLong from value {}'.format(value))
        return super().__new__(cls, value)

    def write(self, stream):
        stream.write(serialisation.pack_unsigned_long(self))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_unsigned_long(stream))


class LongLong(int):
    MIN = -(1 << 63)
    MAX = (1 << 63) - 1

    def __new__(cls, value):
        if not (LongLong.MIN <= value <= LongLong.MAX):
            raise TypeError('Could not construct a LongLong from value {}'.format(value))
        return super().__new__(cls, value)

    def write(self, stream):
        stream.write(serialisation.pack_long_long(self))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_long_long(stream))


class UnsignedLongLong(int):
    MIN = 0
    MAX = (1 << 64) - 1

    def __new__(cls, value):
        if not (UnsignedLongLong.MIN <= value <= UnsignedLongLong.MAX):
            raise TypeError('Could not construct a UnsignedLongLong from value {}'.format(value))
        return super().__new__(cls, value)

    def write(self, stream):
        stream.write(serialisation.pack_unsigned_long_long(self))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_unsigned_long_long(stream))


class ShortStr(str):
    def __new__(cls, value):
        if len(value) > Octet.MAX:
            raise TypeError('Could not construct a ShortStr from value {}'.format(value))
        return super().__new__(cls, value)

    def __hash__(self):
        return super().__hash__()

    def write(self, stream):
        stream.write(serialisation.pack_short_string(self))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_short_string(stream))


class LongStr(str):
    def __new__(cls, value):
        if len(value) > UnsignedLong.MAX:
            raise TypeError('Could not construct a LongStr from value {}'.format(value))
        return super().__new__(cls, value)

    def write(self, stream):
        stream.write(serialisation.pack_long_string(self))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_long_string(stream))


class Table(dict):
    def write(self, stream):
        stream.write(serialisation.pack_table(self))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_table(stream))


class Timestamp(datetime.datetime):
    def __new__(cls, *args, **kwargs):
        if kwargs or len(args) > 1:
            return super().__new__(cls, *args, **kwargs)

        value, = args
        if isinstance(value, datetime.datetime):
            return super().__new__(cls, value.year, value.month, value.day, value.hour, value.minute, value.second)
        raise TypeError("Could not construct a timestamp from value {}".format(value))

    def __eq__(self, other):
        return abs(self - other) < datetime.timedelta(milliseconds=1)

    def write(self, stream):
        stamp = int(self.timestamp())
        stream.write(serialisation.pack_long_long(stamp))

    @classmethod
    def read(cls, stream):
        return cls.fromtimestamp(serialisation.read_long_long(stream))


FIELD_TYPES = {
    'bit': Bit,
    'octet': Octet,
    'short': Short,
    'unsignedshort': UnsignedShort,
    'long': Long,
    'unsignedlong': UnsignedLong,
    'longlong': LongLong,
    'unsignedlonglong': UnsignedLongLong,
    'table': Table,
    'longstr': LongStr,
    'shortstr': ShortStr,
    'timestamp': Timestamp
}
