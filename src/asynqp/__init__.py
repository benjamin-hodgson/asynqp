import asyncio
import enum
import struct
from .exceptions import AMQPError
from .methods import deserialise_method
from . import methods
from . import serialisation


FRAME_END = b'\xCE'


@asyncio.coroutine
def connect(host='localhost', port='5672', username='guest', password='guest', virtual_host='/', ssl=None, *, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()

    # FIXME: the three-way circular dependency between AMQP, Dispatcher and Connection is upsetting.
    #        Better than a two-way dependency between AMQP and Connection, at least...
    dispatcher = Dispatcher()
    transport, protocol = yield from loop.create_connection(lambda: AMQP(dispatcher), host=host, port=port, ssl=ssl)
    connection = Connection(protocol, username, password, virtual_host, loop=loop)
    dispatcher.add_channel(0, connection)

    protocol.send_protocol_header()

    yield from connection.is_open
    return connection


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

        frame = read_frame(frame_type, channel_id, raw_payload)
        self.dispatcher.dispatch(frame)

        # repeat if more than a whole frame was received
        self.data_received(data[8+size:])

    def send_frame(self, frame):
        self.transport.write(frame.serialise())

    def send_protocol_header(self):
        self.transport.write(b'AMQP\x00\x00\x09\x01')


# this class might have to die when I do channels
class Dispatcher(object):
    def __init__(self):
        self.channels = {}

    def add_channel(self, channel_id, channel):
        self.channels[channel_id] = channel

    def dispatch(self, frame):
        channel = self.channels[frame.channel_id]
        try:
            handler = getattr(channel, 'handle_' + type(frame.payload).__name__)
        except AttributeError as e:
            raise AMQPError('No handler configured for method: ' + type(frame.payload).__name__) from e
        else:
            handler(frame)


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
        method = methods.ConnectionTuneOK(1024, 0, 0)  # todo: no magic numbers
        frame = Frame(FrameType.method, 0, method)
        self.protocol.send_frame(frame)

        method = methods.ConnectionOpen(self.virtual_host, '', False)
        frame = Frame(FrameType.method, 0, method)
        self.protocol.send_frame(frame)

    def handle_ConnectionOpenOK(self, frame):
        self.is_open.set_result(True)


def read_frame(frame_type, channel_id, raw_payload):
    if frame_type == 1:
        payload = deserialise_method(raw_payload)
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
