import asyncio
import enum
import struct
from .exceptions import AMQPError
from . import methods
from . import serialisation


FRAME_END = b'\xCE'


class AMQP(asyncio.Protocol):
    def __init__(self, dispatcher):
        self.dispatcher = dispatcher
        self.partial_frame = b''

    def connection_made(self, transport):
        self.transport = transport

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
            self.transport.close()
            raise AMQPError("Frame end byte was incorrect")

        frame = create_frame(frame_type, channel_id, raw_payload)
        self.dispatcher.dispatch(frame)

        # repeat if more than a whole frame was received
        self.data_received(data[8+size:])

    def send_frame(self, frame):
        self.transport.write(frame.serialise())

    def send_protocol_header(self):
        self.transport.write(b'AMQP\x00\x00\x09\x01')


class Dispatcher(object):
    def __init__(self, connection):
        self.connection = connection

    def dispatch(self, frame):
        getattr(self.connection, 'handle_' + type(frame.payload).__name__)(frame)


class Connection(object):
    def __init__(self, protocol, username='guest', password='guest', virtual_host='/', *, loop=None):
        self.protocol = protocol
        self.username = username
        self.password = password
        self.virtual_host = virtual_host
        self.is_open = asyncio.Future(loop=loop)

    def handle_ConnectionStart(self, frame):
        method = methods.ConnectionStartOK({}, 'AMQPLAIN', {'LOGIN': self.username, 'PASSWORD': self.password}, 'en_US')
        frame = Frame(FrameType.method, 0, method)
        self.protocol.send_frame(frame)

    def handle_ConnectionTune(self, frame):
        method = methods.ConnectionTuneOK(1024, 0, 0)
        frame = Frame(FrameType.method, 0, method)
        self.protocol.send_frame(frame)

        method = methods.ConnectionOpen(self.virtual_host)
        frame = Frame(FrameType.method, 0, method)
        self.protocol.send_frame(frame)

    def handle_ConnectionOpenOK(self, frame):
        self.is_open.set_result(True)


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
