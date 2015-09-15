import struct
from .exceptions import AMQPError
from datetime import datetime, timezone


def rethrow_as(expected_cls, to_throw):
    def decorator(f):
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except expected_cls as e:
                raise to_throw from e
        return wrapper
    return decorator


###########################################################
#  Deserialisation
###########################################################


@rethrow_as(struct.error, AMQPError('failed to read an octet'))
def read_octet(stream):
    return _read_octet(stream)[0]


@rethrow_as(struct.error, AMQPError('failed to read a short'))
def read_short(stream):
    return _read_short(stream)[0]


@rethrow_as(struct.error, AMQPError('failed to read an unsigned short'))
def read_unsigned_short(stream):
    return _read_unsigned_short(stream)[0]


@rethrow_as(struct.error, AMQPError('failed to read a long'))
def read_long(stream):
    return _read_long(stream)[0]


@rethrow_as(struct.error, AMQPError('failed to read an unsigned long'))
def read_unsigned_long(stream):
    return _read_unsigned_long(stream)[0]


@rethrow_as(struct.error, AMQPError('failed to read a long long'))
def read_long_long(stream):
    return _read_long_long(stream)[0]


@rethrow_as(struct.error, AMQPError('failed to read an unsigned long long'))
def read_unsigned_long_long(stream):
    return _read_unsigned_long_long(stream)[0]


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


@rethrow_as(KeyError, AMQPError('failed to read an array'))
@rethrow_as(struct.error, AMQPError('failed to read an array'))
def read_array(stream):
    return _read_array(stream)[0]


@rethrow_as(struct.error, AMQPError('failed to read a boolean'))
def read_bool(stream):
    return _read_bool(stream)[0]


@rethrow_as(struct.error, AMQPError('failed to read a boolean'))
def read_bools(byte, number_of_bools):
    bits = "{0:b}".format(byte)
    bits = "0" * (number_of_bools - len(bits)) + bits
    return (b == "1" for b in reversed(bits))


@rethrow_as(struct.error, AMQPError('failed to read a boolean'))
def read_timestamp(stream):
    return _read_timestamp(stream)[0]


def qpid_rabbit_mq_table():
    # TODO: fix amqp 0.9.1 compatibility
    # TODO: Add missing types
    TABLE_VALUE_PARSERS = {
        b't': _read_bool,
        b'b': _read_signed_byte,
        b's': _read_short,
        b'I': _read_long,
        b'l': _read_long_long,
        b'f': _read_float,
        b'S': _read_long_string,
        b'A': _read_array,
        b'V': _read_void,
        b'x': _read_byte_array,
        b'F': _read_table,
        b'T': _read_timestamp
    }
    return TABLE_VALUE_PARSERS


def _read_table(stream):
    TABLE_VALUE_PARSERS = qpid_rabbit_mq_table()
    table = {}

    table_length, initial_long_size = _read_unsigned_long(stream)
    consumed = initial_long_size

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
    str_length, x = _read_unsigned_long(stream)
    buffer = stream.read(str_length)
    if len(buffer) != str_length:
        raise AMQPError("Long string had incorrect length")
    return buffer.decode('utf-8'), x + str_length


def _read_octet(stream):
    x, = struct.unpack('!B', stream.read(1))
    return x, 1


def _read_signed_byte(stream):
    x, = struct.unpack_from('!b', stream.read(1))
    return x, 1


def _read_bool(stream):
    x, = struct.unpack('!?', stream.read(1))
    return x, 1


def _read_short(stream):
    x, = struct.unpack('!h', stream.read(2))
    return x, 2


def _read_unsigned_short(stream):
    x, = struct.unpack('!H', stream.read(2))
    return x, 2


def _read_long(stream):
    x, = struct.unpack('!l', stream.read(4))
    return x, 4


def _read_unsigned_long(stream):
    x, = struct.unpack('!L', stream.read(4))
    return x, 4


def _read_long_long(stream):
    x, = struct.unpack('!q', stream.read(8))
    return x, 8


def _read_unsigned_long_long(stream):
    x, = struct.unpack('!Q', stream.read(8))
    return x, 8


def _read_float(stream):
    x, = struct.unpack('!f', stream.read(4))
    return x, 4


def _read_timestamp(stream):
    x, = struct.unpack('!Q', stream.read(8))
    # From datetime.fromutctimestamp converts it to a local timestamp without timezone information
    return datetime.fromtimestamp(x * 1e-3, timezone.utc), 8


def _read_array(stream):
    TABLE_VALUE_PARSERS = qpid_rabbit_mq_table()
    field_array = []

    # The standard says only long, but unsigned long seems sensible
    array_length, initial_long_size = _read_unsigned_long(stream)
    consumed = initial_long_size

    while consumed < array_length + initial_long_size:
        value_type_code = stream.read(1)
        consumed += 1
        value, x = TABLE_VALUE_PARSERS[value_type_code](stream)
        consumed += x
        field_array.append(value)

    return field_array, consumed


def _read_void(stream):
    return None, 0


def _read_byte_array(stream):
    byte_array_length, x = _read_unsigned_long(stream)
    return stream.read(byte_array_length), byte_array_length + x


###########################################################
#  Serialisation
###########################################################

def pack_short_string(string):
    buffer = string.encode('utf-8')
    return pack_octet(len(buffer)) + buffer


def pack_long_string(string):
    buffer = string.encode('utf-8')
    return pack_unsigned_long(len(buffer)) + buffer


def pack_field_value(value):
    if value is None:
        return b'V'
    if isinstance(value, bool):
        return b't' + pack_bool(value)
    if isinstance(value, dict):
        return b'F' + pack_table(value)
    if isinstance(value, list):
        return b'A' + pack_array(value)
    if isinstance(value, bytes):
        return b'x' + pack_byte_array(value)
    if isinstance(value, str):
        return b'S' + pack_long_string(value)
    if isinstance(value, datetime):
        return b'T' + pack_timestamp(value)
    if isinstance(value, int):
        if value.bit_length() < 8:
            return b'b' + pack_signed_byte(value)
        if value.bit_length() < 32:
            return b'I' + pack_long(value)
    if isinstance(value, float):
        return b'f' + pack_float(value)
    raise NotImplementedError()


def pack_table(d):
    buffer = b''
    for key, value in d.items():
        buffer += pack_short_string(key)
        # todo: more values
        buffer += pack_field_value(value)

    return pack_unsigned_long(len(buffer)) + buffer


def pack_octet(number):
    return struct.pack('!B', number)


def pack_signed_byte(number):
    return struct.pack('!b', number)


def pack_unsigned_byte(number):
    return struct.pack('!B', number)


def pack_short(number):
    return struct.pack('!h', number)


def pack_unsigned_short(number):
    return struct.pack('!H', number)


def pack_long(number):
    return struct.pack('!l', number)


def pack_unsigned_long(number):
    return struct.pack('!L', number)


def pack_long_long(number):
    return struct.pack('!q', number)


def pack_unsigned_long_long(number):
    return struct.pack('!Q', number)


def pack_float(number):
    return struct.pack('!f', number)


def pack_bool(b):
    return struct.pack('!?', b)


def pack_timestamp(timeval):
    number = int(timeval.timestamp() * 1e3)
    return struct.pack('!Q', number)


def pack_byte_array(value):
    buffer = pack_unsigned_long(len(value))
    buffer += value
    return buffer


def pack_array(items):
    buffer = b''
    for value in items:
        buffer += pack_field_value(value)

    return pack_unsigned_long(len(buffer)) + buffer


def pack_bools(*bs):
    tot = 0
    for n, b in enumerate(bs):
        x = 1 if b else 0
        tot += (x << n)
    return pack_octet(tot)
