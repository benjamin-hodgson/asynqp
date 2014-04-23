import asyncio
import enum
import struct


def write_protocol_header(writer):
    writer.write(b'AMQP\x00\x00\x09\x01')


@asyncio.coroutine
def read_frame(reader):
    bytes = yield from reader.read(7)
    frame_type, channel_id, size = struct.unpack('!BHL', bytes)

    raw_payload = yield from reader.read(size)
    frame_end = yield from reader.read(1)
    if frame_end !=  b'\xCE':
        raise AMQPError("Frame end byte was incorrect")


    method_type = MethodType(struct.unpack('!HH', raw_payload[0:4]))

    return Frame(FrameType(frame_type), channel_id, Payload(method_type, raw_payload[4:]))


class Frame(object):
    def __init__(self, frame_type, channel_id, payload):
        self.frame_type = frame_type
        self.channel_id = channel_id
        self.payload = payload


class Payload(object):
    def __init__(self, method_type, arguments):
        self.method_type = method_type
        self.arguments = arguments


class FrameType(enum.Enum):
    method = 1


class MethodType(enum.Enum):
    connection_start = (10, 10)
    connection_start_ok = (10, 11)
    connection_tune = (10, 30)


class AMQPError(IOError):
    pass


class FrameBuilder(object):
    def __init__(self):
        self.payload = b''

    def add_bytes_to_payload(self, bytes):
        self.payload += bytes

    def add_short_string_to_payload(self, bytes):
        self.payload += short_string(bytes)

    def add_table_to_payload(self, d):
        self.payload += table(d)


    def build(self):
        frame = b'\x01'  # frame is a method
        frame += struct.pack('!H', 0)  # channel 0
        frame += long_string(self.payload)
        frame += b'\xCE'  # frame_end
        return frame


def short_string(bytes):
    return struct.pack('!B', len(bytes)) + bytes

def long_string(bytes):
    return struct.pack('!L', len(bytes)) + bytes

def table(d):
    ret = b''
    for k in d:
        ret += short_string(k)
        ret += b'S'
        ret += long_string(d[k])
    return long_string(ret)
