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
        self.connection.dispatch(frame)

        remainder = data[8+size:]
        if remainder:
            self.data_received(remainder)

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
        self.opened = asyncio.Future(loop=self.loop)
        self.closed = asyncio.Future(loop=self.loop)
        self.closing = False
        self.heartbeat_timeout_callback = None
        self.handler = ConnectionFrameHandler(self)
        self.handlers = {0: self.handler}

    @asyncio.coroutine
    def open_channel(self):
        next_channel_num = max(self.handlers.keys()) + 1
        channel = Channel(self.protocol, next_channel_num, loop=self.loop)

        self.handlers[next_channel_num] = channel
        self.protocol.send_frame(Frame(FrameType.method, next_channel_num, methods.ChannelOpen('')))

        yield from channel.opened
        return channel

    @asyncio.coroutine
    def close(self):
        self.send_close()
        yield from self.closed

    def send_heartbeat(self):
        if self.heartbeat_interval > 0:
            self.send_frame(Frame(FrameType.heartbeat, 0, b''))
            self.loop.call_later(self.heartbeat_interval, self.send_heartbeat)

    def monitor_heartbeat(self):
        if self.heartbeat_interval > 0:
            self.heartbeat_timeout_callback = self.loop.call_later(self.heartbeat_interval * 2, self.send_close, 501, 'Heartbeat timed out')

    def reset_heartbeat_timeout(self):
        if self.heartbeat_timeout_callback is not None:
            self.heartbeat_timeout_callback.cancel()
            self.monitor_heartbeat()

    def send_close(self, reply_code=0, reply_text='Connection closed by application', class_id=0, method_id=0):
        frame = self.make_method_frame(methods.ConnectionClose(reply_code, reply_text, class_id, method_id))
        self.send_frame(frame)
        self.closing = True

    def dispatch(self, frame):
        if self.closing and type(frame.payload) not in (methods.ConnectionClose, methods.ConnectionCloseOK):
            return
        if frame.frame_type == FrameType.heartbeat:
            return
        handler = self.handlers[frame.channel_id]
        return handler.handle(frame)

    def send_frame(self, frame):
        self.protocol.send_frame(frame)

    @classmethod
    def make_method_frame(cls, method):
        return Frame(FrameType.method, 0, method)


class ConnectionFrameHandler(object):
    def __init__(self, connection):
        self.connection = connection

    def handle(self, frame):
        method_type = type(frame.payload)

        if frame.frame_type == FrameType.heartbeat:
            return
        else:
            method_name = method_type.__name__

        try:
            handler = getattr(self, 'handle_' + method_name)
        except AttributeError as e:
            raise AMQPError('No handler defined for {} on the connection'.format(method_name)) from e
        else:
            handler(frame)

    def handle_ConnectionStart(self, frame):
        method = methods.ConnectionStartOK(
            {},
            'AMQPLAIN',
            {'LOGIN': self.connection.username, 'PASSWORD': self.connection.password},
            'en_US'
        )
        frame = self.make_method_frame(method)
        self.connection.send_frame(frame)

    def handle_ConnectionTune(self, frame):
        # just agree with whatever the server wants. Make this configurable in future
        if 0 < frame.payload.channel_max < 1024:
            self.connection.max_channel = frame.payload.channel_max.value
        self.connection.heartbeat_interval = frame.payload.heartbeat.value
        self.connection.frame_max = frame.payload.frame_max.value

        self.connection.send_heartbeat()

        method = methods.ConnectionTuneOK(self.connection.max_channel, self.connection.frame_max, self.connection.heartbeat_interval)
        reply_frame = self.make_method_frame(method)
        self.connection.send_frame(reply_frame)

                                                                                    # 'reserved' args
        open_frame = self.make_method_frame(methods.ConnectionOpen(self.connection.virtual_host, '', False))
        self.connection.send_frame(open_frame)

        self.connection.monitor_heartbeat()

    def handle_ConnectionOpenOK(self, frame):
        self.connection.opened.set_result(True)

    def handle_ConnectionClose(self, frame):
        method = methods.ConnectionCloseOK()
        frame = self.make_method_frame(method)
        self.connection.send_frame(frame)
        self.connection.closing = True

    def handle_ConnectionCloseOK(self, frame):
        self.connection.protocol.transport.close()
        self.connection.closed.set_result(True)

    @classmethod
    def make_method_frame(cls, method):
        return Frame(FrameType.method, 0, method)


class Channel(object):
    def __init__(self, protocol, channel_id, *, loop=None):
        self.protocol = protocol
        self.channel_id = channel_id
        self.loop = asyncio.get_event_loop() if loop is None else loop

        self.opened = asyncio.Future(loop=self.loop)
        self.closed = asyncio.Future(loop=self.loop)
        self.closing = False

    @asyncio.coroutine
    def close(self):
        frame = Frame(FrameType.method, self.channel_id, methods.ChannelClose(0, 'Channel closed by application', 0, 0))
        self.protocol.send_frame(frame)
        self.closing = True
        yield from self.closed

    def handle(self, frame):
        method_type = type(frame.payload)
        handle_name = method_type.__name__
        if self.closing and method_type not in (methods.ChannelClose, methods.ChannelCloseOK):
            return

        try:
            handler = getattr(self, 'handle_' + handle_name)
        except AttributeError as e:
            raise AMQPError('No handler defined for {} on channel {}'.format(handle_name, self.channel_id)) from e
        else:
            handler(frame)

    def handle_ChannelOpenOK(self, frame):
        self.opened.set_result(True)

    def handle_ChannelClose(self, frame):
        self.closing = True
        frame = Frame(FrameType.method, self.channel_id, methods.ChannelCloseOK())
        self.protocol.send_frame(frame)

    def handle_ChannelCloseOK(self, frame):
        self.closed.set_result(True)


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
    heartbeat = 8
