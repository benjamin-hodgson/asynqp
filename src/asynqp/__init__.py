import asyncio
import enum
import struct
from .exceptions import AMQPError
from . import methods
from . import serialisation


FRAME_END = b'\xCE'


class AMQP(asyncio.Protocol):
    def __init__(self, connection):
        self.dispatcher = Dispatcher(connection)
        self.partial_frame = b''

    def data_received(self, data):
        data = self.partial_frame + data
        self.partial_frame = b''

        if len(data) < 7:
            self.partial_frame = data
            return

        frame_header = data[:7]
        frame_type, channel_id, size = struct.unpack('!BHL', frame_header)

        if len(data) < size + 8:
            self.partial_frame = data
            return

        raw_payload = data[7:7+size]
        frame_end = data[7+size:8+size]

        if frame_end != FRAME_END:
            raise AMQPError("Frame end byte was incorrect")

        frame = create_frame(frame_type, channel_id, raw_payload)
        self.dispatcher.dispatch(frame)

        # repeat if more than a whole frame was received
        self.data_received(data[8+size:])


class Dispatcher(object):
    def __init__(self, connection):
        self.connection = connection

    def dispatch(self, frame):
        getattr(self.connection, 'handle_' + type(frame.payload).__name__)(frame)


class Connection(object):
    def __init__(self, reader, writer, username='guest', password='guest', virtual_host='/', *, loop=None):
        self.reader = reader
        self.writer = writer
        self.username = username
        self.password = password
        self.virtual_host = virtual_host
        self.is_open = asyncio.Future(loop=loop)

    def write_protocol_header(self):
        self.writer.write(b'AMQP\x00\x00\x09\x01')

    def handle(self, frame):
        getattr(self, 'handle_' + type(frame.payload).__name__)(frame)

    def handle_ConnectionStart(self, frame):
        method = methods.ConnectionStartOK({}, 'AMQPLAIN', {'LOGIN': self.username, 'PASSWORD': self.password}, 'en_US')
        frame = Frame(FrameType.method, 0, method)
        self.write_frame(frame)

    def handle_ConnectionTune(self, frame):
        method = methods.ConnectionTuneOK(1024, 0, 0)
        frame = Frame(FrameType.method, 0, method)
        self.write_frame(frame)

        method = methods.ConnectionOpen(self.virtual_host)
        frame = Frame(FrameType.method, 0, method)
        self.write_frame(frame)

    def handle_ConnectionOpenOK(self, frame):
        self.is_open.set_result(True)

    def write_frame(self, frame):
        self.writer.write(frame.serialise())

    @asyncio.coroutine
    def read_frame(self):
        frame_header = yield from self.reader.read(7)
        frame_type, channel_id, size = struct.unpack('!BHL', frame_header)
        raw_payload = yield from self.reader.read(size)
        frame_end = yield from self.reader.read(1)
        if frame_end != FRAME_END:
            raise AMQPError("Frame end byte was incorrect")
        return create_frame(frame_type, channel_id, raw_payload)


def create_frame(frame_type, channel_id, raw_payload):
    if frame_type == 1:
        payload = methods.deserialise_method(raw_payload)
    return Frame(FrameType(frame_type), channel_id, payload)



class Frame(object):
    def __init__(self, frame_type, channel_id, payload):
        self.frame_type = frame_type
        self.channel_id = channel_id
        self.payload = payload

    def serialise(self):
        payload = self.payload.serialise()
        frame = serialisation.pack_octet(self.frame_type.value)
        frame += serialisation.pack_short(self.channel_id)
        body = self.payload.serialise()
        frame += serialisation.pack_long(len(body)) + body
        frame += FRAME_END  # frame_end
        return frame

    def __eq__(self, other):
        return (self.frame_type == other.frame_type
            and self.channel_id == other.channel_id
            and self.payload == other.payload)


class FrameType(enum.Enum):
    method = 1
