import datetime
from . import serialisation


MAX_OCTET = 0xFF
MAX_SHORT = 0xFFFF
MAX_LONG = 0xFFFFFFFF
MAX_LONG_LONG = 0xFFFFFFFFFFFFFFFF


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
    def __new__(cls, value):
        if not (0 <= value <= MAX_OCTET):
            raise TypeError('Could not construct an Octet from value {}'.format(value))
        return super().__new__(cls, value)

    def write(self, stream):
        stream.write(serialisation.pack_octet(self))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_octet(stream))


class Short(int):
    def __new__(cls, value):
        if not (0 <= value <= MAX_SHORT):
            raise TypeError('Could not construct a Short from value {}'.format(value))
        return super().__new__(cls, value)

    def write(self, stream):
        stream.write(serialisation.pack_short(self))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_short(stream))


class Long(int):
    def __new__(cls, value):
        if not (0 <= value <= MAX_LONG):
            raise TypeError('Could not construct a Long from value {}'.format(value))
        return super().__new__(cls, value)

    def write(self, stream):
        stream.write(serialisation.pack_long(self))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_long(stream))


class LongLong(int):
    def __new__(cls, value):
        if not (0 <= value <= MAX_LONG_LONG):
            raise TypeError('Could not construct a LongLong from value {}'.format(value))
        return super().__new__(cls, value)

    def write(self, stream):
        stream.write(serialisation.pack_long_long(self))

    @classmethod
    def read(cls, stream):
        return cls(serialisation.read_long_long(stream))


class ShortStr(str):
    def __new__(cls, value):
        if len(value) > MAX_OCTET:
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
        if len(value) > MAX_LONG:
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
        return abs(self - other) < datetime.timedelta(seconds=1)

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
    'long': Long,
    'longlong': LongLong,
    'table': Table,
    'longstr': LongStr,
    'shortstr': ShortStr,
    'timestamp': Timestamp
}
