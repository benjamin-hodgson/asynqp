import asyncio
import enum
import struct


def write_protocol_header(writer):
    writer.write(b'AMQP\x00\x00\x09\x01')


class AMQPFrameReader(object):
    def __init__(self, reader):
        self.reader = reader

    @asyncio.coroutine
    def read_frame(self):
        bytes = yield from self.reader.read(7)
        frame_type, channel_id, size = struct.unpack('!BHL', bytes)

        payload = yield from self.reader.read(size)
        frame_end = yield from self.reader.read(1)
        if frame_end !=  b'\xCE':
            raise AMQPError("Frame end byte was incorrect")

        return Frame(frame_type, channel_id, payload)


class Frame(object):
    def __init__(self, frame_type, channel_id, payload):
        self.frame_type = FrameType(frame_type)
        self.channel_id = channel_id
        self.payload = Payload(payload)


class Payload(object):
    def __init__(self, raw):
        self.raw = raw
        self.method = Method(struct.unpack('!HH', raw[0:4]))


class FrameType(enum.Enum):
    method = 1


class Method(enum.Enum):
    start = (10, 10)


class AMQPError(IOError):
    pass
