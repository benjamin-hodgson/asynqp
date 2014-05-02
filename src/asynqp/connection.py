import asyncio
from .channel import Channel
from . import spec
from .exceptions import AMQPError


class ConnectionInfo(object):
    def __init__(self, username, password, virtual_host):
        self.username = username
        self.password = password
        self.virtual_host = virtual_host


class Connection(object):
    def __init__(self, loop, protocol, dispatcher, connection_info):
        self.loop = loop
        self.protocol = protocol
        self.dispatcher = dispatcher
        self.handler = ConnectionFrameHandler(self.protocol, self.loop, connection_info)
        self.opened = self.handler.opened
        self.closed = self.handler.closed

        self.next_channel_num = 1
        self.handler.closing.add_done_callback(self.dispatcher.closing.set_result)  # bit hacky
        self.dispatcher.add_handler(0, self.handler)

    @asyncio.coroutine
    def open_channel(self):
        """
        Open a new channel on this connection.
        This method is a coroutine.

        Return value:
            The new channel object
        """
        channel = Channel(self.protocol, self.next_channel_num, self.dispatcher, loop=self.loop)

        self.protocol.send_method(self.next_channel_num, spec.ChannelOpen(''))
        self.next_channel_num += 1

        yield from channel.opened
        return channel

    @asyncio.coroutine
    def close(self):
        """
        Close the connection by handshaking with the server.
        This method is a coroutine
        """
        self.handler.closing.set_result(True)
        self.protocol.send_method(0, spec.ConnectionClose(0, 'Connection closed by application', 0, 0))
        yield from self.handler.closed


class ConnectionFrameHandler(object):
    def __init__(self, protocol, loop, connection_info):
        self.protocol = protocol
        self.loop = loop
        self.connection_info = connection_info
        self.opened = asyncio.Future(loop=loop)
        self.closing = asyncio.Future(loop=loop)
        self.closed = asyncio.Future(loop=loop)

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
            {'LOGIN': self.connection_info.username, 'PASSWORD': self.connection_info.password},
            'en_US'
        )
        self.protocol.send_method(0, method)

    def handle_ConnectionTune(self, frame):  # just agree with whatever the server wants. Make this configurable in future
        method = spec.ConnectionTuneOK(frame.payload.channel_max, frame.payload.frame_max, frame.payload.heartbeat)
        self.protocol.send_method(0, method)
        self.protocol.send_method(0, spec.ConnectionOpen(self.connection_info.virtual_host, '', False))
        self.protocol.start_heartbeat(frame.payload.heartbeat)

    def handle_ConnectionOpenOK(self, frame):
        self.opened.set_result(True)

    def handle_ConnectionClose(self, frame):
        self.closing.set_result(True)
        self.protocol.send_method(0, spec.ConnectionCloseOK())

    def handle_ConnectionCloseOK(self, frame):
        self.protocol.transport.close()
        self.closed.set_result(True)
