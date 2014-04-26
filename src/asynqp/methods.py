import enum
import struct
from io import BytesIO
from . import serialisation


def create_method(raw_payload):
    method_type_code = struct.unpack('!HH', raw_payload[0:4])
    return METHOD_TYPES[method_type_code].deserialise(raw_payload[4:])


class ConnectionStart(object):
    def __init__(self, major_version, minor_version, server_properties, mechanisms, locales):
        self.method_type = MethodType.connection_start
        self.major_version = major_version
        self.minor_version = minor_version
        self.server_properties = server_properties
        self.mechanisms = mechanisms
        self.locales = locales

    @classmethod
    def deserialise(cls, arguments):
        stream = BytesIO(arguments)
        major_version, minor_version = struct.unpack('!BB', stream.read(2))
        server_properties = serialisation.read_table(stream)
        mechanisms = set(serialisation.read_long_string(stream).split(' '))
        locales = set(serialisation.read_long_string(stream).split(' '))
        return cls(major_version, minor_version, server_properties, mechanisms, locales)


class ConnectionStartOK(object):
    def __init__(self, arguments):
        self.method_type = MethodType.connection_start_ok
        self.arguments = arguments

    def serialise(self):
        body = struct.pack('!HH', *self.method_type.value) + self.arguments
        return serialisation.pack_long(len(body)) + body


class ConnectionTune(object):
    def __init__(self, arguments):
        self.method_type = MethodType.connection_tune
        self.arguments = arguments

    def serialise(self):
        body = struct.pack('!HH', *self.method_type.value) + self.arguments
        return serialisation.pack_long(len(body)) + body

    @classmethod
    def deserialise(cls, arguments):
        return cls(arguments)


class ConnectionTuneOK(object):
    def __init__(self, arguments):
        self.method_type = MethodType.connection_tune_ok
        self.arguments = arguments

    def serialise(self):
        body = struct.pack('!HH', *self.method_type.value) + self.arguments
        return serialisation.pack_long(len(body)) + body


class ConnectionOpen(object):
    def __init__(self, arguments):
        self.method_type = MethodType.connection_open
        self.arguments = arguments

    def serialise(self):
        body = struct.pack('!HH', *self.method_type.value) + self.arguments
        return serialisation.pack_long(len(body)) + body


class ConnectionOpenOK(object):
    def __init__(self, arguments):
        self.method_type = MethodType.connection_open_ok
        self.arguments = arguments

    def serialise(self):
        body = struct.pack('!HH', *self.method_type.value) + self.arguments
        return serialisation.pack_long(len(body)) + body

    @classmethod
    def deserialise(cls, arguments):
        return cls(arguments)


METHOD_TYPES = {
    (10,10): ConnectionStart,
    (10,11): ConnectionStartOK,
    (10,30): ConnectionTune,
    (10,31): ConnectionTuneOK,
    (10,40): ConnectionOpen,
    (10,41): ConnectionOpenOK,
}


class MethodType(enum.Enum):
    connection_start = (10, 10)
    connection_start_ok = (10, 11)
    connection_tune = (10, 30)
    connection_tune_ok = (10, 31)
    connection_open = (10, 40)
    connection_open_ok = (10, 41)
