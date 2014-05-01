import asyncio
import struct
from .exceptions import AMQPError
from . import frames
from . import spec


@asyncio.coroutine
def connect(host='localhost', port='5672', username='guest', password='guest', virtual_host='/', ssl=None, *, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()

    dispatcher = Dispatcher()
    transport, protocol = yield from loop.create_connection(lambda: AMQP(dispatcher), host=host, port=port, ssl=ssl)
    connection = Connection(protocol, dispatcher, username, password, virtual_host, loop=loop)

    protocol.send_protocol_header()

    yield from dispatcher.handlers[0].opened
    return connection


class AMQP(asyncio.Protocol):
    def __init__(self, dispatcher):
        self.dispatcher = dispatcher
        self.partial_frame = b''
        self.heartbeat_monitor = None

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        if self.heartbeat_monitor is not None:
            self.heartbeat_monitor.reset_heartbeat_timeout()  # the spec says 'any octet may substitute for a heartbeat'

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
        frame_end = data[7+size]

        if frame_end != spec.FRAME_END:
            self.transport.close()
            raise AMQPError("Frame end byte was incorrect")

        frame = frames.read(frame_type, channel_id, raw_payload)
        self.dispatcher.dispatch(frame)

        remainder = data[8+size:]
        if remainder:
            self.data_received(remainder)

    def send_method(self, channel, method):
        frame = frames.MethodFrame(channel, method)
        self.send_frame(frame)

    def send_frame(self, frame):
        self.transport.write(frame.serialise())

    def send_protocol_header(self):
        self.transport.write(b'AMQP\x00\x00\x09\x01')


class Dispatcher(object):
    def __init__(self):
        self.handlers = {}

    def add_handler(self, index, handler):
        self.handlers[index] = handler

    def dispatch(self, frame):
        if isinstance(frame, frames.HeartbeatFrame):
            return
        handler = self.handlers[frame.channel_id]
        return handler.handle(frame)


class Connection(object):
    def __init__(self, protocol, dispatcher, username='guest', password='guest', virtual_host='/', *, loop=None):
        self.loop = asyncio.get_event_loop() if loop is None else loop
        self.protocol = protocol
        self.dispatcher = dispatcher
        self.handler = ConnectionFrameHandler(self.protocol, self.loop, {'username': username, 'password': password, 'virtual_host': virtual_host})
        self.next_channel_num = 1
        self.dispatcher.add_handler(0, self.handler)

    @asyncio.coroutine
    def open_channel(self):
        channel = Channel(self.protocol, self.next_channel_num, loop=self.loop)

        self.dispatcher.add_handler(self.next_channel_num, channel)
        self.protocol.send_method(self.next_channel_num, spec.ChannelOpen(''))
        self.next_channel_num += 1

        yield from channel.opened
        return channel

    @asyncio.coroutine
    def close(self):
        self.protocol.send_method(0, spec.ConnectionClose(0, 'Connection closed by application', 0, 0))
        yield from self.handler.closed


class ConnectionFrameHandler(object):
    def __init__(self, protocol, loop, connection_info):
        self.protocol = protocol
        self.loop = loop
        self.connection_info = connection_info
        self.opened = asyncio.Future()
        self.closed = asyncio.Future()

    def handle(self, frame):
        method_type = type(frame.payload)
        method_name = method_type.__name__

        try:
            handler = getattr(self, 'handle_' + method_name)
        except AttributeError as e:
            raise AMQPError('No handler defined for {} on the connection'.format(method_name)) from e
        else:
            handler(frame)

    def handle_ConnectionStart(self, frame):
        method = spec.ConnectionStartOK(
            {},  # TODO
            'AMQPLAIN',
            {'LOGIN': self.connection_info['username'], 'PASSWORD': self.connection_info['password']},
            'en_US'
        )
        self.protocol.send_method(0, method)

    def handle_ConnectionTune(self, frame):  # just agree with whatever the server wants. Make this configurable in future
        self.protocol.heartbeat_monitor = HeartbeatMonitor(self.protocol, self.loop, frame.payload.heartbeat)
        self.protocol.heartbeat_monitor.send_heartbeat()

        method = spec.ConnectionTuneOK(frame.payload.channel_max, frame.payload.frame_max, frame.payload.heartbeat)
        self.protocol.send_method(0, method)

        self.protocol.send_method(0, spec.ConnectionOpen(self.connection_info['virtual_host'], '', False))
        self.protocol.heartbeat_monitor.monitor_heartbeat()

    def handle_ConnectionOpenOK(self, frame):
        self.opened.set_result(True)

    def handle_ConnectionClose(self, frame):
        self.protocol.send_method(0, spec.ConnectionCloseOK())

    def handle_ConnectionCloseOK(self, frame):
        self.protocol.transport.close()
        self.closed.set_result(True)

    def send_method(self, method):
        frame = frames.MethodFrame(0, method)
        self.protocol.send_frame(frame)


class HeartbeatMonitor(object):
    def __init__(self, protocol, loop, heartbeat_interval):
        self.protocol = protocol
        self.loop = loop
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout_callback = None

    def send_heartbeat(self):
        if self.heartbeat_interval > 0:
            self.protocol.send_frame(frames.HeartbeatFrame())
            self.loop.call_later(self.heartbeat_interval, self.send_heartbeat)

    def monitor_heartbeat(self):
        if self.heartbeat_interval > 0:
            close_frame = frames.MethodFrame(0, spec.ConnectionClose(501, 'Heartbeat timed out', 0, 0))
            self.heartbeat_timeout_callback = self.loop.call_later(self.heartbeat_interval * 2, self.protocol.send_frame, close_frame)

    def reset_heartbeat_timeout(self):
        if self.heartbeat_timeout_callback is not None:
            self.heartbeat_timeout_callback.cancel()
            self.monitor_heartbeat()


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
        frame = frames.MethodFrame(self.channel_id, spec.ChannelClose(0, 'Channel closed by application', 0, 0))
        self.protocol.send_frame(frame)
        self.closing = True
        yield from self.closed

    def handle(self, frame):
        method_type = type(frame.payload)
        handle_name = method_type.__name__
        if self.closing and method_type not in (spec.ChannelClose, spec.ChannelCloseOK):
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
        frame = frames.MethodFrame(self.channel_id, spec.ChannelCloseOK())
        self.protocol.send_frame(frame)

    def handle_ChannelCloseOK(self, frame):
        self.closed.set_result(True)
