import struct
from .exceptions import AMQPError
from .util import rethrow_as


###########################################################
#  Deserialisation
###########################################################


@rethrow_as(struct.error, AMQPError('failed to read an octet'))
def read_octet(stream):
    return _read_octet(stream)[0]


@rethrow_as(struct.error, AMQPError('failed to read a short'))
def read_short(stream):
    return _read_short(stream)[0]


@rethrow_as(struct.error, AMQPError('failed to read a long'))
def read_long(stream):
    return _read_long(stream)[0]


@rethrow_as(struct.error, AMQPError('failed to read a long'))
def read_long_long(stream):
    return _read_long_long(stream)[0]


@rethrow_as(struct.error, AMQPError('failed to read a short string'))
def read_short_string(stream):
    return _read_short_string(stream)[0]


@rethrow_as(struct.error, AMQPError('failed to read a long string'))
def read_long_string(stream):
    return _read_long_string(stream)[0]


@rethrow_as(KeyError, AMQPError('failed to read a table'))
@rethrow_as(struct.error, AMQPError('failed to read a table'))
def read_table(stream):
    return _read_table(stream)[0]


@rethrow_as(struct.error, AMQPError('failed to read a table'))
def read_bool(stream):
    return _read_bool(stream)[0]


def _read_table(stream):
    # TODO: more value types
    TABLE_VALUE_PARSERS = {
        b't': _read_bool,
        b's': _read_short_string,
        b'S': _read_long_string,
        b'F': _read_table
    }

    consumed = 0
    table = {}

    table_length, initial_long_size = _read_long(stream)
    consumed += initial_long_size

    while consumed < table_length + initial_long_size:
        key, x = _read_short_string(stream)
        consumed += x

        value_type_code = stream.read(1)
        consumed += 1

        value, x = TABLE_VALUE_PARSERS[value_type_code](stream)
        consumed += x

        table[key] = value

    return table, consumed


def _read_short_string(stream):
    str_length, x = _read_octet(stream)
    string = stream.read(str_length).decode('utf-8')
    return string, x + str_length


def _read_long_string(stream):
    str_length, x = _read_long(stream)
    bytestring = stream.read(str_length)
    if len(bytestring) != str_length:
        raise AMQPError("Long string had incorrect length")
    return bytestring.decode('utf-8'), x + str_length


def _read_octet(stream):
    x, = struct.unpack('!B', stream.read(1))
    return x, 1


def _read_bool(stream):
    x, = struct.unpack('!?', stream.read(1))
    return x, 1


def _read_short(stream):
    x, = struct.unpack('!H', stream.read(2))
    return x, 2


def _read_long(stream):
    x, = struct.unpack('!L', stream.read(4))
    return x, 4


def _read_long_long(stream):
    x, = struct.unpack('!Q', stream.read(8))
    return x, 8


###########################################################
#  Serialisation
###########################################################

def pack_short_string(string):
    bytes = string.encode('utf-8')
    return pack_octet(len(bytes)) + bytes


def pack_long_string(string):
    bytes = string.encode('utf-8')
    return pack_long(len(bytes)) + bytes


def pack_table(d):
    bytes = b''
    for key, value in d.items():
        if not isinstance(value, str):
            raise NotImplementedError()
        bytes += pack_short_string(key)
        bytes += b'S'  # todo: more values
        bytes += pack_long_string(value)
    return pack_long(len(bytes)) + bytes


def pack_octet(number):
    return struct.pack('!B', number)


def pack_short(number):
    return struct.pack('!H', number)


def pack_long(number):
    return struct.pack('!L', number)


def pack_long_long(number):
    return struct.pack('!Q', number)


def pack_bools(*bs):
    tot = 0
    for n, b in enumerate(bs):
        x = 1 if b else 0
        tot += (x << n)
    return pack_octet(tot)
