import asyncio
import enum
import struct
from io import BytesIO
from .exceptions import AMQPError
from . import methods
from . import serialisation


FRAME_END = b'\xCE'


@asyncio.coroutine
def connect(host='localhost', port='5672', username='guest', password='guest', virtual_host='/', ssl=None, *, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()

    # the two-way dependency between AMQP and Connection is unsettling. fixable? not sure
    connection = Connection(username, password, virtual_host, loop=loop)
    transport, protocol = yield from loop.create_connection(lambda: AMQP(connection), host=host, port=port, ssl=ssl)
    connection.protocol = protocol

    protocol.send_protocol_header()

    yield from connection.opened
    return connection


class AMQP(asyncio.Protocol):
    def __init__(self, connection):
        self.connection = connection
        self.partial_frame = b''

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self.connection.reset_heartbeat_timeout()
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

        frame = Frame.read(frame_type, channel_id, raw_payload)
        self.connection.handle(frame)

        # repeat if more than a whole frame was received
        self.data_received(data[8+size:])

    def send_frame(self, frame):
        self.transport.write(frame.serialise())

    def send_protocol_header(self):
        self.transport.write(b'AMQP\x00\x00\x09\x01')


class Connection(object):
    def __init__(self, username='guest', password='guest', virtual_host='/', max_channel=1024, *, loop=None):
        self.loop = asyncio.get_event_loop() if loop is None else loop
        self.protocol = None
        self.username = username
        self.password = password
        self.virtual_host = virtual_host
        self.max_channel = max_channel
        self.opened = asyncio.Future(loop=loop)
        self.closing = asyncio.Future(loop=loop)
        self.channels = {0: self}
        self.heartbeat_timeout_callback = None

    def close(self, reply_code=0, reply_text='Connection closed by application', class_id=0, method_id=0):
        frame = self.make_method_frame(methods.ConnectionClose(reply_code, reply_text, class_id, method_id))
        self.protocol.send_frame(frame)
        self.closing.set_result(True)

    def send_heartbeat(self):
        self.protocol.send_frame(Frame(FrameType.heartbeat, 0, b''))
        self.loop.call_later(self.heartbeat_interval, self.send_heartbeat)

    def reset_heartbeat_timeout(self):
        if self.heartbeat_timeout_callback is not None:
            self.heartbeat_timeout_callback.cancel()
            self.heartbeat_timeout_callback = self.loop.call_later(self.heartbeat_interval * 2, self.close, 501, 'Heartbeat timed out')

    def handle(self, frame):
        method_type = type(frame.payload)
        if self.closing.done() and method_type not in (methods.ConnectionClose, methods.ConnectionCloseOK):
            return

        channel = self.channels[frame.channel_id]

        if frame.frame_type == FrameType.heartbeat:
            return
        else:
            handle_name = method_type.__name__

        try:
            handler = getattr(channel, 'handle_' + handle_name)
        except AttributeError as e:
            raise AMQPError('No handler defined for ' + handle_name) from e
        else:
            handler(frame)

    def handle_ConnectionStart(self, frame):
        method = methods.ConnectionStartOK({}, 'AMQPLAIN', {'LOGIN': self.username, 'PASSWORD': self.password}, 'en_US')
        frame = self.make_method_frame(method)
        self.protocol.send_frame(frame)

    def handle_ConnectionTune(self, frame):
        # just agree with whatever the server wants. Make this configurable in future
        if 0 < frame.payload.channel_max < 1024:
            self.max_channel = frame.payload.channel_max.value
        self.heartbeat_interval = frame.payload.heartbeat.value
        self.frame_max = frame.payload.frame_max.value

        self.loop.call_later(self.heartbeat_interval, self.send_heartbeat)

        method = methods.ConnectionTuneOK(self.max_channel, self.frame_max, self.heartbeat_interval)
        reply_frame = self.make_method_frame(method)
        self.protocol.send_frame(reply_frame)

                                                                                    # 'reserved' args
        open_frame = self.make_method_frame(methods.ConnectionOpen(self.virtual_host, '', False))
        self.protocol.send_frame(open_frame)

        self.heartbeat_timeout_callback = self.loop.call_later(self.heartbeat_interval * 2, self.close, 501, 'Heartbeat timed out')

    def handle_ConnectionOpenOK(self, frame):
        self.opened.set_result(True)

    def handle_ConnectionClose(self, frame):
        method = methods.ConnectionCloseOK()
        frame = self.make_method_frame(method)
        self.protocol.send_frame(frame)
        self.closing.set_result(True)

    def handle_ConnectionCloseOK(self, frame):
        self.protocol.transport.close()

    @classmethod
    def make_method_frame(cls, method):
        return Frame(FrameType.method, 0, method)


class Frame(object):
    def __init__(self, frame_type, channel_id, payload):
        self.frame_type = frame_type
        self.channel_id = channel_id
        self.payload = payload

    def serialise(self):
        frame = serialisation.pack_octet(self.frame_type.value)
        frame += serialisation.pack_short(self.channel_id)

        if isinstance(self.payload, bytes):
            body = self.payload
        else:
            bytesio = BytesIO()
            self.payload.write(bytesio)
            body = bytesio.getvalue()

        frame += serialisation.pack_long(len(body)) + body
        frame += FRAME_END
        return frame

    def __eq__(self, other):
        return (self.frame_type == other.frame_type
            and self.channel_id == other.channel_id
            and self.payload == other.payload)

    @classmethod
    def read(cls, frame_type, channel_id, raw_payload):
        if frame_type == 1:
            payload = methods.read_method(raw_payload)
        return cls(FrameType(frame_type), channel_id, payload)


class FrameType(enum.Enum):
    method = 1
    heartbeat = 4
