import asyncio
from . import spec
from . import queue
from .exceptions import AMQPError


class Channel(object):
    """
    A Channel is a 'virtual connection' over which messages are sent and received.
    Several independent channels can be multiplexed over the same connection,
    so peers can perform several tasks concurrently while using a single socket.

    Multi-threaded applications will typically use a 'channel-per-thread' approach,
    though it is also acceptable to open several connections for a single thread.

    Attributes:
        channel.channel_id: the numerical ID of the channel
        channel.opened: a Future which is done when the handshake to open the channel has finished
        channel.closing: a Future which is done when the handshake to close the channel has been initiated
        channel.closed: a Future which is done when the handshake to close the channel has finished

    Methods:
        channel.close(): Close the channel. This method is a coroutine.
    """
    def __init__(self, protocol, channel_id, dispatcher, loop):
        self.channel_id = channel_id
        self.loop = loop

        self.sender = ChannelMethodSender(channel_id, protocol)

        self.handler = ChannelFrameHandler(channel_id, self.sender, loop)
        self.opened = self.handler.opened
        self.closing = self.handler.closing
        self.closed = self.handler.closed
        dispatcher.add_handler(channel_id, self.handler)

    @asyncio.coroutine
    def declare_queue(self, name, *, durable=True, exclusive=False, auto_delete=False):
        self.handler.queue_declared = asyncio.Future(loop=self.loop)
        self.sender.send_QueueDeclare(name, durable, exclusive, auto_delete)
        name = yield from self.handler.queue_declared
        return queue.Queue(name, durable, exclusive, auto_delete)

    @asyncio.coroutine
    def close(self):
        """
        Close the channel by handshaking with the server.
        This method is a coroutine.
        """
        self.closing.set_result(True)
        self.sender.send_Close(0, 'Channel closed by application', 0, 0)
        yield from self.closed


class ChannelFrameHandler(object):
    def __init__(self, channel_id, sender, loop):
        self.channel_id = channel_id
        self.sender = sender

        self.opened = asyncio.Future(loop=loop)
        self.closing = asyncio.Future(loop=loop)
        self.closed = asyncio.Future(loop=loop)

        self.queue_declared = None

    def handle(self, frame):
        method_type = type(frame.payload)
        handle_name = method_type.__name__
        if self.closing.done() and method_type not in (spec.ChannelClose, spec.ChannelCloseOK):
            return

        try:
            handler = getattr(self, 'handle_' + handle_name)
        except AttributeError as e:
            raise AMQPError('No handler defined for {} on channel {}'.format(handle_name, self.channel_id)) from e
        else:
            handler(frame)

    def handle_ChannelOpenOK(self, frame):
        self.opened.set_result(True)

    def handle_QueueDeclareOK(self, frame):
        name = frame.payload.queue
        self.queue_declared.set_result(name)

    def handle_ChannelClose(self, frame):
        self.closing.set_result(True)
        self.sender.send_CloseOK()

    def handle_ChannelCloseOK(self, frame):
        self.closed.set_result(True)


class ChannelMethodSender(object):
    def __init__(self, channel_id, protocol):
        self.channel_id = channel_id
        self.protocol = protocol

    def send_QueueDeclare(self, name, durable, exclusive, auto_delete):
        self.protocol.send_method(self.channel_id, spec.QueueDeclare(0, name, False, durable, exclusive, auto_delete, False, {}))

    def send_Close(self, status_code, message, class_id, method_id):
        self.protocol.send_method(self.channel_id, spec.ChannelClose(0, 'Channel closed by application', 0, 0))

    def send_CloseOK(self):
        self.protocol.send_method(self.channel_id, spec.ChannelCloseOK())
