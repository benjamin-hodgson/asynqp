import abc
import struct
from io import BytesIO
from . import serialisation
from .exceptions import AMQPError


def deserialise_method(raw_payload):
    method_type_code = struct.unpack('!HH', raw_payload[0:4])
    return METHOD_TYPES[method_type_code].deserialise(raw_payload)


class IncomingMethod(abc.ABC):
    """
    Base class for methods which arrive from the server
    """
    @classmethod
    def deserialise(cls, raw):
        stream = BytesIO(raw)
        method_type = struct.unpack('!HH', stream.read(4))
        if method_type != cls.method_type:
            raise AMQPError("How did this happen? Wrong method type for {}: {}".format(cls.__name__, method_type))
        return cls.read(stream)

    @classmethod
    @abc.abstractmethod
    def read(cls, stream):
        """
        Read a method from a binary file-like object.
        """


class OutgoingMethod(abc.ABC):
    """
    Base class for methods which are sent to the server
    """
    def serialise(self):
        stream = BytesIO(struct.pack('!HH', *self.method_type))
        stream.seek(0, 2)
        self.write(stream)
        return stream.getvalue()

    @abc.abstractmethod
    def write(self, stream):
        """
        Write the method to a binary file-like object.
        """


class ConnectionStart(IncomingMethod):
    method_type = (10, 10)

    def __init__(self, major_version, minor_version, server_properties, mechanisms, locales):
        self.version = (major_version, minor_version)
        self.server_properties = server_properties
        self.mechanisms = mechanisms
        self.locales = locales

    @classmethod
    def read(cls, stream):
        major_version, minor_version = struct.unpack('!BB', stream.read(2))
        server_properties = serialisation.read_table(stream)
        mechanisms = set(serialisation.read_long_string(stream).split(' '))
        locales = set(serialisation.read_long_string(stream).split(' '))
        return cls(major_version, minor_version, server_properties, mechanisms, locales)


class ConnectionStartOK(OutgoingMethod):
    method_type = (10, 11)

    def __init__(self, client_properties, mechanism, security_response, locale):
        self.client_properties = client_properties
        self.mechanism = mechanism
        self.security_response = security_response
        self.locale = locale

    def write(self, stream):
        stream.write(serialisation.pack_table(self.client_properties))
        stream.write(serialisation.pack_short_string(self.mechanism))
        stream.write(serialisation.pack_table(self.security_response))
        stream.write(serialisation.pack_short_string(self.locale))


class ConnectionTune(IncomingMethod):
    method_type = (10, 30)

    def __init__(self, max_channel, max_frame_length, heartbeat_interval):
        self.max_channel = max_channel
        self.max_frame_length = max_frame_length
        self.heartbeat_interval = heartbeat_interval

    @classmethod
    def read(cls, stream):
        max_channel = serialisation.read_short(stream)
        max_frame_length = serialisation.read_long(stream)
        heartbeat_interval = serialisation.read_short(stream)
        return cls(max_channel, max_frame_length, heartbeat_interval)


class ConnectionTuneOK(OutgoingMethod):
    method_type = (10, 31)

    def __init__(self, max_channel, max_frame_length, heartbeat_interval):
        self.max_channel = max_channel
        self.max_frame_length = max_frame_length
        self.heartbeat_interval = heartbeat_interval

    def write(self, stream):
        stream.write(serialisation.pack_short(self.max_channel))
        stream.write(serialisation.pack_long(self.max_frame_length))
        stream.write(serialisation.pack_short(self.heartbeat_interval))


class ConnectionOpen(OutgoingMethod):
    method_type = (10, 40)

    def __init__(self, virtual_host):
        self.virtual_host = virtual_host

    def write(self, stream):
        stream.write(serialisation.pack_short_string(self.virtual_host))
        stream.write(serialisation.pack_short_string(''))  # reserved-1
        stream.write(b'\x00')  # reserved-2


class ConnectionOpenOK(IncomingMethod):
    method_type = (10, 41)

    @classmethod
    def read(cls, stream):
        serialisation.read_short_string(stream)  # reserved-1
        return cls()

    def __eq__(self, other):
        return type(self) is type(other)


METHOD_TYPES = {
    (10,10): ConnectionStart,
    (10,11): ConnectionStartOK,
    (10,30): ConnectionTune,
    (10,31): ConnectionTuneOK,
    (10,40): ConnectionOpen,
    (10,41): ConnectionOpenOK,
}
