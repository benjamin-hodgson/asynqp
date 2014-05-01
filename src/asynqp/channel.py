import asyncio
from . import frames
from . import spec
from .exceptions import AMQPError


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
        """
        Close the channel by handshaking with the server.
        This method is a coroutine.
        """
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
